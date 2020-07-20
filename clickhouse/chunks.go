package clickhouse

import (
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type (
	readResult struct {
		result []byte
		err    error
	}

	pendingRequest struct {
		id int32

		// request params
		cancelFunc func()        // must be called to release context resources
		bodyReader io.ReadCloser // must be closed after request is done

		// channel where results are streamed to
		// when channel is closed it means that all results are fully read
		resCh chan *readResult

		// internal state
		buf []byte
	}
)

var (
	bigRequestID    int32
	pendingRequests = struct {
		sync.Mutex
		m map[int32]*pendingRequest
	}{
		m: make(map[int32]*pendingRequest),
	}

	errNoSuchQuery = errors.New("No such query pending (perhaps timed out)")
)

func addPendingRequest(cancelFunc func(), bodyReader io.ReadCloser, buf []byte) int32 {
	reqID := atomic.AddInt32(&bigRequestID, 1)

	pendingRequests.Lock()
	defer pendingRequests.Unlock()

	req := &pendingRequest{
		id:         reqID,
		cancelFunc: cancelFunc,
		bodyReader: bodyReader,
		buf:        buf,
		resCh:      make(chan *readResult, 1),
	}

	pendingRequests.m[reqID] = req
	go req.loop()

	return reqID
}

func getNextChunk(id int32) (buf []byte, eof bool, err error) {
	pendingRequests.Lock()
	req, ok := pendingRequests.m[id]
	pendingRequests.Unlock()

	if !ok {
		return nil, false, errNoSuchQuery
	}

	res, ok := <-req.resCh
	if !ok {
		return nil, true, nil
	}

	return res.result, false, res.err
}

func (r *pendingRequest) loop() {
	const readTimeout = time.Second * 15

	defer func() {
		pendingRequests.Lock()
		delete(pendingRequests.m, r.id)
		pendingRequests.Unlock()

		r.cancelFunc()
		r.bodyReader.Close()
	}()

	r.resCh <- &readResult{result: r.buf}
	r.buf = nil

	for {
		buf := make([]byte, maxResultSize)
		n, err := io.ReadFull(r.bodyReader, buf)

		res := &readResult{result: buf[0:n], err: err}

		if err == io.EOF {
			break
		} else if err == io.ErrUnexpectedEOF {
			res.err = nil
		}

		select {
		case <-time.After(readTimeout):
			log.Printf("Request %d timed out while waiting for readers", r.id)
			close(r.resCh)
			return
		case r.resCh <- res:
		}

		if err != nil {
			break
		}
	}

	close(r.resCh)

	// let readers fetch all the results
	time.Sleep(readTimeout)
}
