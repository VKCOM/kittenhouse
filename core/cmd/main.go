package cmd

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // this is effectively a main package
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vkcom/engine-go/srvfunc"
	"github.com/vkcom/kittenhouse/core/clickhouse"
	"github.com/vkcom/kittenhouse/core/cmdconfig"
	"github.com/vkcom/kittenhouse/core/destination"
	"github.com/vkcom/kittenhouse/core/inmem"
	"github.com/vkcom/kittenhouse/core/persist"
)

var (
	// StartServerCallback is the callback you need to use to use your own RPC protocol instead of HTTP
	StartServerCallback func(host string, port uint) error

	// Build* can be filled in during build using go build -ldflags
	BuildTime    string
	BuildOSUname string
	BuildCommit  string
	buildVersion string // concatination of Build* into a single string
)

const (
	FlagPersistent = 1 << iota
	FlagDebug
	FlagRowBinary

	ErrCodeNotSupported = 300

	heartbeatInterval = time.Hour
	maxUDPPacketSize  = 2048 // even less, actually
	debug             = false
)

func init() {
	buildVersion = fmt.Sprintf(`kittenhouse compiled at %s by %s after %s on %s`, BuildTime, runtime.Version(),
		BuildCommit, BuildOSUname,
	)

	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)
}

func updateThread(ch chan os.Signal) {
	for range ch {
		updateConfig()
		reopenLog()
	}
}

var (
	oldConf        destination.Map
	configUpdateTs int32        // UNIX ts
	configHash     atomic.Value // string
)

func updateConfig() {
	log.Println("Updating config")

	var newConf destination.Map
	var ts time.Time
	var confHash string
	var err error

	if argv.config != "" {
		newConf, ts, confHash, err = parseConfigFile(argv.config)
		if err != nil {
			log.Printf("Error: Bad config: %s", err.Error())
			return
		}
	} else {
		ts = time.Now()
		newConf, confHash, err = parseConfig(bytes.NewBufferString(`* ` + argv.chHost))
		if err != nil {
			log.Printf("Error: Bad default config: %s", err.Error())
			return
		}
	}

	if oldConf != nil {
		for _, settings := range oldConf {
			settings.Destroy()
		}
	}

	oldConf = newConf
	atomic.StoreInt32(&configUpdateTs, int32(ts.Unix()))
	configHash.Store(confHash)

	inmem.UpdateDestinationsConfig(newConf)
	persist.UpdateDestinationsConfig(newConf)
	clickhouse.UpdateDestinationsConfig(newConf)
}

func reopenLog() {
	if argv.log == "" {
		return
	}

	var err error
	logFd, err = srvfunc.LogRotate(logFd, argv.log)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf(`Cannot log to file "%s": %s`, argv.log, err.Error()))
		return
	}

	log.SetOutput(logFd)
}

func tryIncreaseRlimit() {
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Printf("Could not get rlimit: %s", err.Error())
		return
	}

	rLimit.Max = argv.maxOpenFiles
	rLimit.Cur = argv.maxOpenFiles

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Printf("Could not set new rlimit: %s", err.Error())
		return
	}
}

func avgCPU(old, new syscall.Timeval, duration time.Duration) float32 {
	if duration <= 0 {
		return 0
	}

	return float32(float64(new.Nano()-old.Nano()) / float64(duration*time.Nanosecond))
}

func heartbeatThread() {
	var oldRusage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &oldRusage)
	prevTs := time.Now()

	for {
		var curRusage syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &curRusage)

		var rss uint64
		if st, _ := srvfunc.GetMemStat(0); st != nil {
			rss = st.Res
		}

		dur := time.Since(prevTs)
		configHashStr, _ := configHash.Load().(string)

		persist.Heartbeat(
			buildVersion,
			BuildCommit,
			atomic.LoadInt32(&configUpdateTs),
			configHashStr,
			rss,
			avgCPU(oldRusage.Utime, curRusage.Utime, dur),
			avgCPU(oldRusage.Stime, curRusage.Stime, dur),
		)

		time.Sleep(heartbeatInterval)
	}
}

// Main is actual main function for kittenhouse but allows to register certain hooks beforehand.
func Main() {
	if argv.version {
		fmt.Fprint(os.Stderr, buildVersion, "\n")
		return
	} else if argv.help {
		flag.Usage()
		return
	}

	if argv.nProc > 0 {
		runtime.GOMAXPROCS(int(argv.nProc))
	} else {
		argv.nProc = uint(runtime.NumCPU())
	}

	if argv.pprofHostPort != `` {
		go func() {
			if err := http.ListenAndServe(argv.pprofHostPort, nil); err != nil {
				log.Printf(`pprof listen fail: %s`, err.Error())
			}
		}()
	}

	tryIncreaseRlimit()

	if argv.group != "" {
		if err := srvfunc.ChangeGroup(argv.group); err != nil {
			log.Fatalf("Could not change group to %s: %s", argv.group, err.Error())
		}
	}

	if argv.user != "" {
		if err := srvfunc.ChangeUser(argv.user); err != nil {
			log.Fatalf("Could not change user to %s: %s", argv.user, err.Error())
		}
	}

	if argv.reverse {
		listenAddr := fmt.Sprintf("%s:%d", argv.host, argv.port)
		log.Printf("Starting reverse proxy at %s (proxy to %s)", listenAddr, argv.chHost)
		err := clickhouse.RunReverseProxy(listenAddr, argv.chHost)
		log.Fatalf("Could not run reverse proxy: %s", err.Error())
	}

	clickhouse.Init()

	persist.Init(persist.Config{
		Dir:            argv.dir,
		MaxSendSize:    argv.maxSendSize,
		MaxFileSize:    argv.maxFileSize,
		RotateInterval: time.Duration(argv.rotateIntervalSec) * time.Second,
		MarkAsDone:     argv.markAsDone,
		Port:           argv.port,
	})

	persist.InternalLog("start", "", 0, "", "version: "+buildVersion+" args:"+fmt.Sprint(os.Args))

	updCh := make(chan os.Signal, 10)
	signal.Notify(updCh, syscall.SIGHUP, syscall.SIGUSR1, syscall.SIGUSR2)
	reopenLog()
	updateConfig()
	go updateThread(updCh)
	go heartbeatThread()

	go func() {
		if err := StartServerCallback(argv.host, argv.port); err != nil {
			log.Fatalf("Could not listen rpc: %s", err.Error())
		}

		log.Printf("Listening %s:%d (TCP)", argv.host, argv.port)
	}()

	go listenUDP()

	sigChan := make(chan os.Signal, 10)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	<-sigChan

	log.Printf("Flushing offsets map")
	if err := persist.FlushAcknowlegedOffsetsMap(); err != nil {
		log.Printf("Could not flush acknowledged offsets map: %s", err.Error())
	}
}
