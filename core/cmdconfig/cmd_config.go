package cmd

import (
	"flag"
	"os"
)

var (
	argv struct {
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
	flag.StringVar(&argv.chHost, `ch-addr`, `127.0.0.1:8123`, `default clickhouse host:port`)
	flag.UintVar(&argv.nProc, `cores`, uint(0), `max cpu cores usage`)
	flag.StringVar(&argv.pprofHostPort, `pprof`, ``, `host:port for http pprof`)
	flag.Uint64Var(&argv.maxOpenFiles, `max-open-files`, 262144, `open files limit`)
	flag.StringVar(&argv.chDatabase, `db`, `default`, "clickhouse database")
	flag.StringVar(&argv.chUser, `ch-user`, ``, "clickhouse user")
	flag.StringVar(&argv.chPassword, `ch-password`, ``, "clickhouse password")
	flag.StringVar(&argv.chSslCertPath, `ch-ssl-cert-path`, ``, "clickhouse SSL cert path")

	// local proxy options
	flag.StringVar(&argv.config, `c`, ``, `path to routing config`)
	flag.StringVar(&argv.dir, `dir`, `/tmp/kittenhouse`, `dir for persistent logs`)
	flag.Int64Var(&argv.maxSendSize, `max-send-size`, 1<<20, `max batch size to be sent to clickhouse in bytes`)
	flag.Int64Var(&argv.maxFileSize, `max-file-size`, 50<<20, `max file size in bytes`)
	flag.Int64Var(&argv.rotateIntervalSec, `rotate-interval-sec`, 1800, `how often to rotate files`)
	flag.BoolVar(&argv.markAsDone, `mark-as-done`, false, `rename files to *.done instead of deleting them upon successful delivery`)

	flag.Parse()
}