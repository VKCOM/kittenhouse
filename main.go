package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vkcom/kittenhouse/clickhouse"
	"github.com/vkcom/kittenhouse/destination"
	"github.com/vkcom/kittenhouse/inmem"
	"github.com/vkcom/kittenhouse/persist"
	"github.com/vkcom/kittenhouse/config"
	"github.com/vkcom/engine-go/srvfunc"
)

var (
	//Build* filled in assembly go build -ldflags
	BuildTime    string
	BuildOSUname string
	BuildCommit  string
	buildVersion string // join Build* into single string
)

var (
	argv struct {
		reverse bool

		disableKittenMeow bool

		host       string
		port       uint
		help       bool
		version    bool
		markAsDone bool
		user       string
		group      string
		log        string
		logDebug   bool

		maxOpenFiles      uint64
		nProc             uint
		pprofHostPort     string
		chHost            string
		config            string
		dir               string
		maxBufferSize     int64
		maxSendSize       int64
		maxFileSize       int64
		rotateIntervalSec int64
	}

	logFd *os.File
)

const (
	FlagPersistent = 1 << 0
	FlagDebug      = 1 << 1
	FlagRowBinary  = 1 << 2
)



const (
	heartbeatInterval = time.Hour
	maxUDPPacketSize  = 8192 // does not make sense to make it much more than that
)

func init() {
	buildVersion = fmt.Sprintf(`kittenhouse compiled at %s by %s after %s on %s`, BuildTime, runtime.Version(),
		BuildCommit, BuildOSUname,
	)

	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	// actions
	flag.BoolVar(&argv.help, `h`, false, `show this help`)
	flag.BoolVar(&argv.version, `version`, false, `show version`)
	flag.BoolVar(&argv.reverse, `reverse`, false, `start reverse proxy server instead (ch-addr is used as clickhouse host-port)`)

	// common options
	flag.StringVar(&argv.host, `host`, `0.0.0.0`, `listening host`)
	flag.UintVar(&argv.port, `port`, 13338, `listening port. REQUIRED`)
	flag.UintVar(&argv.port, `p`, 13338, `listening port. REQUIRED`)
	flag.StringVar(&argv.user, `u`, `kitten`, "setuid user (if needed)")
	flag.StringVar(&argv.group, `g`, `kitten`, "setgid user (if needed)")
	flag.StringVar(&argv.log, `l`, "", "log file (if needed)")
	flag.BoolVar(&argv.logDebug, `debug-log`, false, "extra verbosity log")
	flag.StringVar(&argv.chHost, `ch-addr`, `127.0.0.1:8123`, `default clickhouse host:port`)
	flag.UintVar(&argv.nProc, `cores`, uint(0), `max cpu cores usage`)
	flag.StringVar(&argv.pprofHostPort, `pprof`, ``, `host:port for http pprof`)
	flag.Uint64Var(&argv.maxOpenFiles, `max-open-files`, 262144, `open files limit`)
	flag.BoolVar(&argv.disableKittenMeow, `disable-kitten-meow`, false, `Allow KITTEN/MEOW protocol between direct & reverse kh`)

	// local proxy options
	flag.StringVar(&argv.config, `c`, ``, `path to routing config`)
	flag.StringVar(&argv.dir, `dir`, `/tmp/clickhouse_proxy`, `dir for persistent logs`)
	flag.Int64Var(&argv.maxSendSize, `max-send-size`, 1<<20, `max batch size to be sent to clickhouse in bytes`)
	flag.Int64Var(&argv.maxBufferSize, `max-buffer-size`, 16<<20, `max per-table in-memory buffer size in bytes`)
	flag.Int64Var(&argv.maxFileSize, `max-file-size`, 50<<20, `max file size in bytes`)
	flag.Int64Var(&argv.rotateIntervalSec, `rotate-interval-sec`, 1800, `how often to rotate files`)
	flag.BoolVar(&argv.markAsDone, `mark-as-done`, false, `rename files to *.done instead of deleting them upon successful delivery`)

	flag.Parse()

	debug := argv.logDebug
	persist.WriteDebugLog = debug
	inmem.WriteDebugLog = debug
	clickhouse.WriteDebugLog = debug

	clickhouse.KittenMeowDisabled = argv.disableKittenMeow
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
		newConf, ts, confHash, err = config.ParseConfigFile(argv.config)
		if err != nil {
			log.Printf("Error: Bad config: %s", err.Error())
			return
		}
	} else {
		ts = time.Now()
		newConf, confHash, err = config.ParseConfig(bytes.NewBufferString(`* ` + argv.chHost))
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

func main() {
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

	inmem.Init(inmem.Config{
		MaxBufferSize: argv.maxBufferSize,
	})

	persist.InternalLog("start", "", 0, "", "version: "+buildVersion+" args:"+fmt.Sprint(os.Args))

	updCh := make(chan os.Signal, 10)
	signal.Notify(updCh, syscall.SIGHUP, syscall.SIGUSR1, syscall.SIGUSR2)
	reopenLog()
	updateConfig()
	go updateThread(updCh)
	go heartbeatThread()

	serveHTTP()
	log.Printf("Listening %s:%d", argv.host, argv.port)

	go listenUDP()

	sigChan := make(chan os.Signal, 10)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigChan

	persist.InternalLog("stop", "", 0, "", fmt.Sprintf("got %v signal", sig))

	log.Printf("Flushing offsets map")
	if err := persist.FlushAcknowlegedOffsetsMap(); err != nil {
		log.Printf("Could not flush acknowledged offsets map: %s", err.Error())
	}
}
