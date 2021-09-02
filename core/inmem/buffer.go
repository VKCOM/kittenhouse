package inmem

import (
	"bytes"
	"errors"
	"log"
	"sync"
	"sync/atomic"

	"github.com/Tirael666/kittenhouse/core/kittenerror"
	"github.com/Tirael666/kittenhouse/core/persist"
)

type (
	multiWriteBuf struct {
		mu        sync.Mutex
		values    *writeBuf
		rowBinary *writeBuf
	}

	writeBuf struct {
		b              bytes.Buffer
		overflowLogged bool
	}
)

const (
	ErrCodeBufferOverflow = 200

	maxBufSize = 16 << 20
)

var (
	errOverflow = errors.New("Writing too fast: buffer overflow")

	overflowCount   int64
	flushErrorCount int64
	totalTraffic    int64

	tableBufMap = struct {
		sync.RWMutex // Lock to element must be taken before unlocking the map. See more full explanation below.
		v            map[string]*multiWriteBuf
	}{
		v: make(map[string]*multiWriteBuf),
	}
)

// Write adds data to per-table buffer if there is enough capacity.
// locks are taken in the following order:
//   - map.RLock()
//   - el = map[table]
//   - el.Lock()
//   - map.RUnlock()
//   - doSometing(el)
//   - el.Unlock()
//
// It is done because map is periodically swapped to empty map
// and we need to ensure that we finished writing to el that was taken after map was unlocked.
func Write(table string, data []byte, rowBinary bool) error {
	tableBufMap.RLock()
	res, ok := tableBufMap.v[table]

	// fast path
	if ok {
		res.mu.Lock()
		tableBufMap.RUnlock()
		err := res.write(table, data, rowBinary)
		res.mu.Unlock()
		return err
	}

	tableBufMap.RUnlock()
	tableBufMap.Lock()

	// map can be modified between RUnlock() and Lock() so we need to check again
	res, ok = tableBufMap.v[table]
	if !ok {
		res = &multiWriteBuf{}
		tableBufMap.v[table] = res
	}

	res.mu.Lock()
	tableBufMap.Unlock()
	err := res.write(table, data, rowBinary)
	res.mu.Unlock()

	return err
}

func (m *multiWriteBuf) write(table string, data []byte, rowBinary bool) error {
	if rowBinary {
		if m.rowBinary == nil {
			m.rowBinary = &writeBuf{}
		}
		return m.rowBinary.write(table, data, rowBinary)
	}

	if m.values == nil {
		m.values = &writeBuf{}
	}

	return m.values.write(table, data, rowBinary)
}

func (buf *writeBuf) write(table string, data []byte, rowBinary bool) error {
	if buf.b.Len()+len(data)+1 >= maxBufSize {
		buf.logOverflow(table)
		return kittenerror.NewCustom(ErrCodeBufferOverflow, "Writing too fast: buffer overflow", "")
	}

	if !rowBinary && buf.b.Len() > 0 {
		buf.b.WriteByte(',')
	}

	buf.b.Write([]byte(data))
	return nil
}

func (buf *writeBuf) logOverflow(table string) {
	if buf.overflowLogged {
		return
	}

	atomic.AddInt64(&overflowCount, 1)
	persist.InternalLog("inmem.overflow", table, 0, "", "")
	log.Printf("Too much data written per second, dropping the rest for table %s", table)
	buf.overflowLogged = true
}
