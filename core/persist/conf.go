package persist

import (
	"log"
	"os"
	"time"

	"github.com/vkcom/kittenhouse/core/destination"
)

// Config for package
type Config struct {
	Dir            string
	Port           uint // for debug purposes
	MaxSendSize    int64
	MaxFileSize    int64
	RotateInterval time.Duration
	MarkAsDone     bool // rename files to ".done" instead of deleting files that are delivered successfully

	host string
}

var (
	conf Config
)

// Init initializes config variables and checks that configuration is valid.
func Init(c Config) {
	conf = c
	conf.host, _ = os.Hostname()

	checkBinLogDirOrDie()
	initAcknowlegedOffsetsMapOrDie()

	go readFilesThread()
}

// UpdateDestinationsConfig accepts destination.Map and restarts all senders
func UpdateDestinationsConfig(m destination.Map) {
	log.Println("Asked to update config for persistent senders")

	senderMap.Lock()
	defer senderMap.Unlock()

	stopAllSenders()
	initSenders(m)
}

// must be holding senderMap.Lock()
func stopAllSenders() {
	var rcvChans []chan struct{}

	log.Println("Stopping all persistent senders")

	for _, s := range senderMap.v {
		ch := make(chan struct{}, 1)
		s.stopCh <- ch
		rcvChans = append(rcvChans, ch)
	}

	for _, ch := range rcvChans {
		<-ch
	}

	log.Println("Persistent senders stopped")
}

// must be holding senderMap.Lock()
// configuration must be already verified (there must exist only single default section, etc)
func initSenders(m destination.Map) {
	senderMap.v = make(map[destination.ServersStr]*sender)

	for dst, settings := range m {
		s := &sender{
			isDefault:      settings.Default,
			dst:            settings,
			tables:         settings.Tables,
			stopCh:         make(chan chan struct{}),
			filesCh:        make(chan []string, 3), // do not want the queue to become too big
			brokenFilesMap: make(map[string]*brokenState),
		}

		senderMap.v[dst] = s
		go s.loop()
	}
}
