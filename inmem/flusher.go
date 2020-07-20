package inmem

import (
	"log"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/vkcom/kittenhouse/clickhouse"
	"github.com/vkcom/kittenhouse/destination"
	"github.com/vkcom/kittenhouse/persist"
)

const (
	flushInterval = 2 * time.Second
)

type flusher struct {
	isDefault     bool
	errCnt        uint
	dst           *destination.Setting
	excludeTables map[string]struct{} // set for default flusher
	includeTables map[string]struct{} // set for non-default flusher
	stopCh        chan struct{}
}

func (f *flusher) loop() {
	for {
		select {
		case <-f.stopCh:
			return
		case <-time.After(time.Second + time.Duration(rand.Intn(2000))*time.Millisecond):
			oldErrCnt := f.errCnt
			f.loopIteration()
			if f.errCnt == oldErrCnt {
				f.errCnt = 0
			} else if f.errCnt >= 1 {
				baseRandSleepInterval := time.Second + time.Duration(rand.Intn(2000))*time.Millisecond
				pow := f.errCnt - 1
				if pow > 5 {
					pow = 5
				}
				time.Sleep(baseRandSleepInterval * time.Duration(1<<pow))
			}
		}
	}
}

func (f *flusher) loopIteration() {
	for table, buf := range f.consumeOurTables() {
		buf.mu.Lock()
		// lock/unlock here is needed to ensure that element is no longer used
		buf.mu.Unlock()

		if buf.values != nil {
			f.flush(table, buf.values, false)
		}

		if buf.rowBinary != nil {
			f.flush(table, buf.rowBinary, true)
		}
	}
}

func (f *flusher) flush(table string, buf *writeBuf, rowBinary bool) {
	flushLen := buf.b.Len()
	if flushLen == 0 {
		return
	}

	if WriteDebugLog {
		log.Printf("Flushing %s (%d bytes)", table, flushLen)
	}

	atomic.AddInt64(&totalTraffic, int64(flushLen))

	// do not care about errors for non-persistent events, so we don't retry
	if err := clickhouse.Flush(f.dst, table, buf.b.Bytes(), rowBinary); err != nil {
		content := buf.b.Bytes()
		if len(content) > 1024 {
			content = content[:1024]
		}

		persist.InternalLog("inmem.flush_error", table, int64(flushLen), err.Error(), string(content))
		atomic.AddInt64(&flushErrorCount, 1)
		f.errCnt++
	}
}

func (f *flusher) consumeOurTables() map[string]*multiWriteBuf {
	tableBufMap.Lock()
	defer tableBufMap.Unlock()

	ourTables := make(map[string]*multiWriteBuf)

	for table, buf := range tableBufMap.v {
		// table = "video_logs_buffer (time,owner_id,video_id,server,pid,level,type,msg,result,extra)"
		// tableIndex = "video_logs_buffer"
		tableIndex := table
		if idx := strings.IndexByte(table, '('); idx >= 0 {
			tableIndex = tableIndex[0:idx]
		}
		tableIndex = strings.TrimSpace(tableIndex)

		if f.isDefault {
			if _, ok := f.excludeTables[tableIndex]; ok {
				continue
			}
		} else {
			if _, ok := f.includeTables[tableIndex]; !ok {
				continue
			}
		}

		ourTables[table] = buf
		delete(tableBufMap.v, table)
	}

	return ourTables
}
