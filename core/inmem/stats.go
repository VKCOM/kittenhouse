package inmem

import (
	"fmt"
	"sync/atomic"
)

// AddStats fills stats keys for inmem storage
func AddStats(m map[string]string) {
	m["INMEM_total_traffic"] = fmt.Sprint(atomic.LoadInt64(&totalTraffic))
	m["INMEM_overflow_count"] = fmt.Sprint(atomic.LoadInt64(&overflowCount))
	m["INMEM_flush_error_count"] = fmt.Sprint(atomic.LoadInt64(&flushErrorCount))
}
