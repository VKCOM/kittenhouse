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
	cmdconfig "github.com/vkcom/kittenhouse/core/cmdconfig"
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

	if cmdconfig.Argv.Config != "" {
		newConf, ts, confHash, err = parseConfigFile(cmdconfig.Argv.Config)
		if err != nil {
			log.Printf("Error: Bad config: %s", err.Error())
			return
		}
	} else {
		ts = time.Now()
		newConf, confHash, err = parseConfig(bytes.NewBufferString(`* ` + cmdconfig.Argv.ChHost))
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
	if cmdconfig.Argv.Log == "" {
		return
	}

	var err error
	logFd, err = srvfunc.LogRotate(cmdconfig.LogFd, cmdconfig.Argv.Log)
	if err != nil {
		os.Stderr.WriteString(fmt.Sprintf(`Cannot log to file "%s": %s`, cmdconfig.Argv.Log, err.Error()))
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

	rLimit.Max = cmdconfig.Argv.MaxOpenFiles
	rLimit.Cur = cmdconfig.Argv.MaxOpenFiles

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
	if cmdconfig.Argv.Version {
		fmt.Fprint(os.Stderr, buildVersion, "\n")
		return
	} else if cmdconfig.Argv.Help {
		flag.Usage()
		return
	}

	if cmdconfig.Argv.NProc > 0 {
		runtime.GOMAXPROCS(int(cmdconfig.Argv.NProc))
	} else {
		cmdconfig.Argv.NProc = uint(runtime.NumCPU())
	}

	if cmdconfig.Argv.PprofHostPort != `` {
		go func() {
			if err := http.ListenAndServe(cmdconfig.Argv.PprofHostPort, nil); err != nil {
				log.Printf(`pprof listen fail: %s`, err.Error())
			}
		}()
	}

	tryIncreaseRlimit()

	if cmdconfig.Argv.Group != "" {
		if err := srvfunc.ChangeGroup(cmdconfig.Argv.Group); err != nil {
			log.Fatalf("Could not change group to %s: %s", cmdconfig.Argv.Group, err.Error())
		}
	}

	if cmdconfig.Argv.User != "" {
		if err := srvfunc.ChangeUser(cmdconfig.Argv.User); err != nil {
			log.Fatalf("Could not change user to %s: %s", cmdconfig.Argv.User, err.Error())
		}
	}

	if cmdconfig.Argv.Reverse {
		listenAddr := fmt.Sprintf("%s:%d", cmdconfig.Argv.Host, cmdconfig.Argv.Port)
		log.Printf("Starting reverse proxy at %s (proxy to %s)", listenAddr, cmdconfig.Argv.ChHost)
		err := clickhouse.RunReverseProxy(listenAddr, cmdconfig.Argv.ChHost)
		log.Fatalf("Could not run reverse proxy: %s", err.Error())
	}

	clickhouse.Init()

	persist.Init(persist.Config{
		Dir:            cmdconfig.Argv.Dir,
		MaxSendSize:    cmdconfig.Argv.MaxSendSize,
		MaxFileSize:    cmdconfig.Argv.MaxFileSize,
		RotateInterval: time.Duration(cmdconfig.Argv.RotateIntervalSec) * time.Second,
		MarkAsDone:     cmdconfig.Argv.MarkAsDone,
		Port:           cmdconfig.Argv.Port,
	})

	persist.InternalLog("start", "", 0, "", "version: "+buildVersion+" args:"+fmt.Sprint(os.Args))

	updCh := make(chan os.Signal, 10)
	signal.Notify(updCh, syscall.SIGHUP, syscall.SIGUSR1, syscall.SIGUSR2)
	reopenLog()
	updateConfig()
	go updateThread(updCh)
	go heartbeatThread()

	go func() {
		if err := StartServerCallback(cmdconfig.Argv.Host, cmdconfig.Argv.Port); err != nil {
			log.Fatalf("Could not listen rpc: %s", err.Error())
		}

		log.Printf("Listening %s:%d (TCP)", cmdconfig.Argv.Host, cmdconfig.Argv.Port)
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
