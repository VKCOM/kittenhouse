package inmem

import (
	"sync"

	"github.com/vkcom/Tirael666/core/destination"
)

const (
	debug = false
)

var flusherMap = struct {
	sync.Mutex
	v map[destination.ServersStr]*flusher
}{
	v: make(map[destination.ServersStr]*flusher),
}

// UpdateDestinationsConfig accepts destination.Map and starts new flushers if neccessary
func UpdateDestinationsConfig(m destination.Map) {
	flusherMap.Lock()
	defer flusherMap.Unlock()

	for _, fl := range flusherMap.v {
		select {
		case fl.stopCh <- struct{}{}:
		default:
		}
	}

	var defaultFlusher *flusher
	flusherMap.v = make(map[destination.ServersStr]*flusher, len(m))
	allTables := make(map[string]struct{})

	for dst, settings := range m {
		fl := &flusher{
			isDefault: settings.Default,
			dst:       settings,
			stopCh:    make(chan struct{}, 1),
		}

		if settings.Default {
			defaultFlusher = fl
		} else {
			fl.includeTables = make(map[string]struct{}, len(settings.Tables))
		}

		for _, table := range settings.Tables {
			allTables[table] = struct{}{}

			if !settings.Default {
				fl.includeTables[table] = struct{}{}
			}
		}
		flusherMap.v[dst] = fl
	}

	if defaultFlusher != nil {
		defaultFlusher.excludeTables = allTables
	}

	for _, fl := range flusherMap.v {
		go fl.loop()
	}
}
