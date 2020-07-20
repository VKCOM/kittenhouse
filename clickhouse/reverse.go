package clickhouse

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
)

const (
	FlagRowBinary = 1 << iota
	FlagLZ4

	errorLogTable                 = "reverse.reverse_proxy_log_buffer(uri, rowbinary, http_code, error_text, post_data)"
	errorLogSampling              = 100 // 1/errorLogSampling errors will be logged directly into ClickHouse
	smallByteSliceSize            = 1 << 16
	maxBodyLen                    = 100 << 20
	maxHappyClickHouseConnections = 100                   // you don't want more, believe me
	flushInterval                 = 25 * time.Millisecond // there can be a lot of tables on a single server
	maxConcurrentReaders          = maxHappyClickHouseConnections
	maxPoolBufferSize             = 256 << 10 // max buffer size that can be returned into a pool
	maxInsertBufferSize           = 10 << 20  // max buffer size for INSERT before flushing
)

var (
	proxyClient       *fasthttp.HostClient
	insertProxyClient *fasthttp.HostClient

	smallByteSlicePool = sync.Pool{New: func() interface{} { return make([]byte, smallByteSliceSize) }}

	errTooBigBody = errors.New("Too big body")

	flushMap = struct {
		m map[string]*flusher // map[requestURI]*flusher
		sync.RWMutex
	}{
		m: make(map[string]*flusher),
	}

	tableFlushMap = struct {
		m map[string]*flusher // map[requestURI]*flusher
		sync.RWMutex
	}{
		m: make(map[string]*flusher),
	}

	rowBinaryTableFlushMap = struct {
		m map[string]*flusher // map[requestURI]*flusher
		sync.RWMutex
	}{
		m: make(map[string]*flusher),
	}

	insertBytes   = []byte("INSERT")
	insertBytesLC = []byte("insert")
)

type request struct {
	buf   []byte
	ackCh chan struct{} // signal that buf can be released
	errCh chan response // request error
}

type response struct {
	statusCode int
	statusText string
}

type flusher struct {
	reqCh            chan request
	readThrottleChan chan struct{}
	uri              string
	rowBinary        bool

	// state
	req        fasthttp.Request
	resp       fasthttp.Response
	wroteBytes int
	pendingCh  []chan response
}

func (f *flusher) loop() {
	ticker := time.Tick(flushInterval)

	f.req.Header.Set("Host", "localhost")
	f.req.Header.SetMethod("POST")
	f.req.SetRequestURI(f.uri)

	f.rowBinary = strings.HasSuffix(f.uri, "RowBinary")

	for {
		select {
		case r := <-f.reqCh:
			f.pendingCh = append(f.pendingCh, r.errCh)
			if !f.rowBinary && f.wroteBytes > 0 {
				f.req.AppendBodyString(",")
			}
			f.wroteBytes += len(r.buf)
			f.req.AppendBody(r.buf)
			r.ackCh <- struct{}{}

			if f.wroteBytes >= maxInsertBufferSize {
				f.flush()
			}
		case <-ticker:
			f.flush()
		}
	}
}

// account for the fact that ClickHouse can sometimes unexpectedly close connection
// and error is not detected by fasthttp as io.EOF
func (f *flusher) tryInsert(req *fasthttp.Request, resp *fasthttp.Response) error {
	var err error

	for i := 0; i < 5; i++ {
		err = insertProxyClient.Do(req, resp)
		if err == nil || !strings.Contains(err.Error(), "write: broken pipe") {
			return err
		}
	}

	return err
}

func (f *flusher) flush() {
	if len(f.pendingCh) == 0 {
		return
	}

	start := time.Now()
	if WriteDebugLog {
		if f.rowBinary {
			log.Printf("Sending request: <RowBinary (%d bytes)>", len(f.req.Body()))
		} else {
			log.Printf("Sending request: %s", f.req.Body())
		}
	}
	var resp response

	err := f.tryInsert(&f.req, &f.resp)

	if WriteDebugLog {
		log.Printf("Request done, took %s", time.Since(start))
	}

	if err != nil {
		resp.statusCode = fasthttp.StatusInternalServerError
		resp.statusText = err.Error()
	} else {
		resp.statusCode = f.resp.StatusCode()
		resp.statusText = string(f.resp.Body())
	}

	if resp.statusCode != http.StatusOK {
		f.logError(resp)
	}

	for _, ch := range f.pendingCh {
		ch <- resp
	}

	f.pendingCh = f.pendingCh[0:0]
	f.req.ResetBody()
	f.resp.ResetBody()
	f.wroteBytes = 0
}

func (f *flusher) logError(resp response) {
	if WriteDebugLog {
		log.Printf(`Flush error. code:%d text:%s`, resp.statusCode, resp.statusText)
	}

	if rand.Intn(errorLogSampling) != 0 {
		return
	}

	req := &fasthttp.Request{}

	req.Header.Set("Host", "localhost")
	req.Header.SetMethod("POST")
	req.SetRequestURI(`/?query=` + url.QueryEscape(`INSERT INTO `+errorLogTable+` VALUES`))

	rowBinary := 0
	if f.rowBinary {
		rowBinary = 1
	}

	req.AppendBodyString(fmt.Sprintf(`('%s', %d, %d, '%s', '%s')`,
		Escape(f.uri), rowBinary, resp.statusCode, Escape(resp.statusText), Escape(string(f.req.Body()))))

	f.tryInsert(req, &fasthttp.Response{})

	req.ResetBody()
}

// copy-paste from https://github.com/valyala/fasthttp/issues/64
func reverseProxyHandler(ctx *fasthttp.RequestCtx) {
	req := &ctx.Request
	resp := &ctx.Response
	req.Header.Del("Connection")
	if err := proxyClient.Do(req, resp); err != nil {
		log.Printf("error when proxying the request: %s", err)
	}
	resp.Header.Del("Connection")
}

func getFlusher(uri string) *flusher {

	flushMap.RLock()
	fl, ok := flushMap.m[uri]
	flushMap.RUnlock()

	if !ok {
		flushMap.Lock()
		fl, ok = flushMap.m[uri]
		if !ok {
			fl = &flusher{
				reqCh:            make(chan request),
				readThrottleChan: make(chan struct{}, maxConcurrentReaders),
				uri:              uri,
			}
			flushMap.m[uri] = fl
		}
		flushMap.Unlock()

		go fl.loop()
	}

	return fl
}

func getFlusherForTable(table string, rowBinary bool) *flusher {
	m := &tableFlushMap
	if rowBinary {
		m = &rowBinaryTableFlushMap
	}

	m.RLock()
	fl, ok := m.m[table]
	m.RUnlock()

	if !ok {
		m.Lock()
		fl, ok = m.m[table]
		if !ok {
			uri := fmt.Sprintf("/?input_format_values_interpret_expressions=0&query=%s", insertQueryPrefixEscaped(table, rowBinary))
			fl = &flusher{
				reqCh:            make(chan request),
				readThrottleChan: make(chan struct{}, maxConcurrentReaders),
				uri:              uri,
			}
			m.m[table] = fl
		}
		m.Unlock()

		go fl.loop()
	}

	return fl
}

func handlePOST(ctx *fasthttp.RequestCtx) {
	query := ctx.QueryArgs().Peek("query")
	if query == nil || !bytes.HasPrefix(query, insertBytes) && !bytes.HasPrefix(query, insertBytesLC) {
		reverseProxyHandler(ctx)
		return
	}

	uri := string(ctx.RequestURI())

	fl := getFlusher(uri)

	fl.readThrottleChan <- struct{}{}

	req := request{
		errCh: make(chan response),
		ackCh: make(chan struct{}),
		buf:   ctx.Request.Body(),
	}
	fl.reqCh <- req
	<-req.ackCh
	ctx.Request.ResetBody()

	<-fl.readThrottleChan

	resp := <-req.errCh

	ctx.SetStatusCode(resp.statusCode)
	ctx.WriteString(resp.statusText)
}

// KITTEN-MEOW protocol for INSERT queries:
// 1. Send a simple request with method KITTEN instead of GET, POST, etc (HTTP/1.1)
// 2. You will receive MEOW in response body. KITTEN-MEOW protocol is now in effect
// 3. Requests:
//      <1 byte>   Flags (e.g. RowBinary flag)
//      <2 bytes>  Table name length in bytes (little endian, unsigned)
//      []byte     Table name contents
//      <4 bytes>  Body length (little endian, unsigned)
//      []byte     Body contents
//
// 4. Response:
//      <1 byte>   Flags
//      <4 bytes>  ClickHouse HTTP Code  (little endian, unsigned)
//      <4 bytes>  Response length (little endian, unsigned)
//      []byte     Response contents

var bytesKitten = []byte("KITTEN")

func debugLogPrintf(msg string, args ...interface{}) {
	if !WriteDebugLog {
		return
	}

	log.Printf(msg, args...)
}

func handleKittenStream(c net.Conn) {
	rd := bufio.NewReader(c)
	wr := bufio.NewWriter(c)
	buf := make([]byte, 4)

	for {
		c.SetReadDeadline(time.Now().Add(time.Minute))

		fl, err := rd.ReadByte()
		if err != nil {
			debugLogPrintf("Could not read flags: %s", err.Error())
			return
		}

		_, err = io.ReadFull(rd, buf[0:2])
		if err != nil {
			debugLogPrintf("Could not read table length: %s", err.Error())
			return
		}

		tblLen := binary.LittleEndian.Uint16(buf[0:2])

		resp, err := kittenStreamSingleRead(rd, fl, tblLen, buf)
		if err != nil {
			debugLogPrintf("Could not read single request: %s", err.Error())
			return
		}

		c.SetWriteDeadline(time.Now().Add(time.Minute))

		err = wr.WriteByte(0) // flags
		if err != nil {
			debugLogPrintf("Could not write response flags: %s", err.Error())
			return
		}

		binary.LittleEndian.PutUint32(buf[0:4], uint32(resp.statusCode))
		_, err = wr.Write(buf[0:4])
		if err != nil {
			debugLogPrintf("Could not write response status code: %s", err.Error())
			return
		}

		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(resp.statusText)))
		_, err = wr.Write(buf[0:4])
		if err != nil {
			debugLogPrintf("Could not write response status text length: %s", err.Error())
			return
		}

		if len(resp.statusText) > 0 {
			_, err = wr.WriteString(resp.statusText)
			if err != nil {
				debugLogPrintf("Could not write response status text: %s", err.Error())
				return
			}
		}

		err = wr.Flush()
		if err != nil {
			debugLogPrintf("Could not flush response: %s", err.Error())
			return
		}
	}
}

func kittenStreamSingleRead(rd *bufio.Reader, flags byte, tblLen uint16, bodyLenBuf []byte) (response, error) {
	buf := smallByteSlicePool.Get().([]byte)
	_, err := io.ReadFull(rd, buf[0:int(tblLen)])
	if err != nil {
		smallByteSlicePool.Put(buf)
		return response{}, err
	}

	tableName := string(buf[0:int(tblLen)])
	smallByteSlicePool.Put(buf)

	fl := getFlusherForTable(tableName, flags&FlagRowBinary == FlagRowBinary)

	_, err = io.ReadFull(rd, bodyLenBuf)
	if err != nil {
		return response{}, err
	}

	bodyLen := binary.LittleEndian.Uint32(bodyLenBuf)
	if bodyLen > maxBodyLen {
		return response{}, errTooBigBody
	}

	var bodyBuf []byte
	var returnToPool bool

	if bodyLen <= smallByteSliceSize {
		bodyBuf = smallByteSlicePool.Get().([]byte)
		returnToPool = true
	} else {
		bodyBuf = make([]byte, bodyLen)
	}

	fl.readThrottleChan <- struct{}{}
	_, err = io.ReadFull(rd, bodyBuf[0:bodyLen])
	if err != nil && err != io.EOF {
		if returnToPool {
			smallByteSlicePool.Put(bodyBuf)
		}
		<-fl.readThrottleChan
		return response{}, err
	}

	req := request{
		errCh: make(chan response),
		ackCh: make(chan struct{}),
		buf:   bodyBuf[0:bodyLen],
	}
	fl.reqCh <- req
	<-req.ackCh
	if returnToPool {
		smallByteSlicePool.Put(bodyBuf)
	}
	<-fl.readThrottleChan

	return <-req.errCh, nil
}

func handleKitten(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(200)
	ctx.WriteString("MEOW")
	ctx.Hijack(handleKittenStream)
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	if bytes.Equal(ctx.Method(), bytesKitten) {
		handleKitten(ctx)
		return
	}

	if ctx.IsPost() {
		handlePOST(ctx)
	} else {
		reverseProxyHandler(ctx)
	}
}

// RunReverseProxy starts a web server that proxies INSERTs and SELECTs to ClickHouse.
// INSERTs are grouped by query text in URL.
//    listenAddr is in form "0.0.0.0:1234"
//    chAddr     is the address of ClickHouse and must be in form "127.0.0.1:2345" as well
func RunReverseProxy(listenAddr, chAddr string) error {
	proxyClient = &fasthttp.HostClient{
		Addr:                chAddr,
		MaxConns:            maxHappyClickHouseConnections,
		MaxIdleConnDuration: time.Second,
	}

	insertProxyClient = &fasthttp.HostClient{
		Addr:                chAddr,
		MaxConns:            maxHappyClickHouseConnections,
		MaxIdleConnDuration: time.Second,
	}

	srv := &fasthttp.Server{
		MaxRequestBodySize: 16 << 20,
		Handler:            requestHandler,
	}

	return srv.ListenAndServe(listenAddr)
}
