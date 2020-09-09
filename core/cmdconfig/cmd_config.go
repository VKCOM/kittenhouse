package cmd

import (
	"flag"
	"os"
)

var (
	Argv struct {
		reverse bool

		host       string
		port       uint
		help       bool
		version    bool
		markAsDone bool
		user       string
		group      string
		log        string

		maxOpenFiles      uint64
		nProc             uint
		pprofHostPort     string
		chHost            string
		chDatabase        string
		chUser            string
		chPassword        string
		chSslCertPath     string
		config            string
		dir               string
		maxSendSize       int64
		maxFileSize       int64
		rotateIntervalSec int64
	}

	logFd *os.File
)

func init() {
	// actions
	flag.BoolVar(&Argv.help, `h`, false, `show this help`)
	flag.BoolVar(&Argv.version, `version`, false, `show version`)
	flag.BoolVar(&Argv.reverse, `reverse`, false, `start reverse proxy server instead (ch-addr is used as clickhouse host-port)`)

	// common options
	flag.StringVar(&Argv.host, `host`, `0.0.0.0`, `listening host`)
	flag.UintVar(&Argv.port, `port`, 13338, `listening port. REQUIRED`)
	flag.UintVar(&Argv.port, `p`, 13338, `listening port. REQUIRED`)
	flag.StringVar(&Argv.user, `u`, `kitten`, "setuid user (if needed)")
	flag.StringVar(&Argv.group, `g`, `kitten`, "setgid user (if needed)")
	flag.StringVar(&Argv.log, `l`, "", "log file (if needed)")
	flag.StringVar(&Argv.chHost, `ch-addr`, `127.0.0.1:8123`, `default clickhouse host:port`)
	flag.UintVar(&Argv.nProc, `cores`, uint(0), `max cpu cores usage`)
	flag.StringVar(&Argv.pprofHostPort, `pprof`, ``, `host:port for http pprof`)
	flag.Uint64Var(&Argv.maxOpenFiles, `max-open-files`, 262144, `open files limit`)
	flag.StringVar(&Argv.chDatabase, `db`, `default`, "clickhouse database")
	flag.StringVar(&Argv.chUser, `ch-user`, ``, "clickhouse user")
	flag.StringVar(&Argv.chPassword, `ch-password`, ``, "clickhouse password")
	flag.StringVar(&Argv.chSslCertPath, `ch-ssl-cert-path`, ``, "clickhouse SSL cert path")

	// local proxy options
	flag.StringVar(&Argv.config, `c`, ``, `path to routing config`)
	flag.StringVar(&Argv.dir, `dir`, `/tmp/kittenhouse`, `dir for persistent logs`)
	flag.Int64Var(&Argv.maxSendSize, `max-send-size`, 1<<20, `max batch size to be sent to clickhouse in bytes`)
	flag.Int64Var(&Argv.maxFileSize, `max-file-size`, 50<<20, `max file size in bytes`)
	flag.Int64Var(&Argv.rotateIntervalSec, `rotate-interval-sec`, 1800, `how often to rotate files`)
	flag.BoolVar(&Argv.markAsDone, `mark-as-done`, false, `rename files to *.done instead of deleting them upon successful delivery`)

	flag.Parse()
}