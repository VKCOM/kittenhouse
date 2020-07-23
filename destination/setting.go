package destination

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

var TestSeed int64 = 0

type (
	// ServersStr represent location that accepts logs (e.g. "srv1:8123" or "=srv1*100 =srv2*100 =srv3*100")
	ServersStr string

	// ServerHostPort is a full host name with port, e.g. "srv1:8123"
	ServerHostPort string

	// Server is a single entry in cluster specification
	Server struct {
		HostPort ServerHostPort
		Weight   uint32
	}

	// Map represents routing map config
	Map map[ServersStr]*Setting

	// Setting represents group of settings for specific destination Server
	Setting struct {
		Default bool
		Servers []Server
		Tables  []string

		// state, all is protected by mu
		mu           sync.Mutex
		curServerIdx int
		curMaxWeight uint32
		brokenHosts  map[ServerHostPort]time.Time
		stopCh       chan struct{}
	}
)

// NewSetting must be used to initialize Setting struct
func NewSetting() *Setting {
	return &Setting{
		brokenHosts: make(map[ServerHostPort]time.Time),
		stopCh:      make(chan struct{}),
	}
}

// Init sets up current state based on public settings
func (s *Setting) Init() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.curServerIdx = -1 // curServerIdx is incremented first, so we need to set it to -1 in order to get 0 first
	s.recalcMaxWeight()
}

// Destroy stops health check goroutines, if any
func (s *Setting) Destroy() {
	close(s.stopCh)
}

// ChooseNextServer returns next server from the list or ok=false, which means that there are no available hosts
func (s *Setting) ChooseNextServer() (srv ServerHostPort, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cnt := len(s.Servers)

	for i := 0; i < cnt; i++ {
		s.curServerIdx = (s.curServerIdx + 1) % cnt
		el := s.Servers[s.curServerIdx]
		if _, ok := s.brokenHosts[el.HostPort]; ok {
			continue
		}

		if s.curMaxWeight > 0 && uint32(rand.Intn(int(s.curMaxWeight))+1) > el.Weight {
			continue
		}

		return el.HostPort, true
	}

	return "", false
}

type aliveCheckFunc func(ServerHostPort) error

// TempDisableHost marks provided host as temporarily disabled (it will not be returned in ChooseNextServer()).
// Provided callback checkCb is used to periodically check that server went live again.
func (s *Setting) TempDisableHost(srv ServerHostPort, aliveCheck aliveCheckFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.brokenHosts[srv]; ok {
		return
	}

	log.Printf("Temporarily disabled %s", srv)

	s.brokenHosts[srv] = time.Now()
	s.recalcMaxWeight()

	go s.tryRestoreHostLoop(srv, aliveCheck)
}

func (s *Setting) tryRestoreHostLoop(srv ServerHostPort, aliveCheck aliveCheckFunc) {
	if WriteDebugLog {
		log.Printf(`START tryRestoreHostLoop for %s`, srv)
		defer func() {
			log.Printf(`FINISH tryRestoreHostLoop for %s`, srv)
		}()
	}

	checkingInterval := 3 * time.Second
	const checkingIntervalMax = 120 * time.Second

	for {
		// smoothly increase time of pause between health check
		checkingIntervalDispersion := time.Duration(float64(checkingInterval) * (1 + rand.Float64()))
		if checkingInterval = 2 * checkingIntervalDispersion; checkingInterval > checkingIntervalMax {
			checkingInterval = checkingIntervalMax
		}

		select {
		case <-time.After(checkingIntervalDispersion):
		case <-s.stopCh:
			return
		}

		log.Printf("Checking whether %s is alive", srv)

		err := aliveCheck(srv)
		if err == nil {
			s.enableHost(srv)
			return
		}

		log.Printf("Health check failed for %s: %s", srv, err.Error())
	}
}

// enableHost allows specified server to be used again
func (s *Setting) enableHost(srv ServerHostPort) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("Server %s is back alive", srv)
	delete(s.brokenHosts, srv)
	s.recalcMaxWeight()
}

// assumes that mu.Lock() is held
func (s *Setting) recalcMaxWeight() {
	s.curMaxWeight = 0

	// shuffle servers so that we do not kill servers in order
	s.Servers = ShuffleServers(s.Servers)

	for _, el := range s.Servers {
		if _, ok := s.brokenHosts[el.HostPort]; ok {
			continue
		}

		if el.Weight > s.curMaxWeight {
			s.curMaxWeight = el.Weight
		}
	}
}

func ShuffleServers(servers []Server) []Server {
	if TestSeed != 0 {
		rand.Seed(TestSeed)
	}
	serversList := append([]Server(nil), servers...)
	for i := range serversList {
		j := rand.Intn(i + 1)
		serversList[i], serversList[j] = serversList[j], serversList[i]
	}
	return serversList
}
