package cmd

import (
	"flag"
	"os"
)

var (
	Argv struct {
		reverse bool

		Host       string
		Port       uint
		help       bool
		version    bool
		MarkAsDone bool
		User       string
		Group      string
		Log        string

		MaxOpenFiles      uint64
		NProc             uint
		PprofHostPort     string
		ChHost            string
		ChDatabase        string
		ChUser            string
		ChPassword        string
		ChSslCertPath     string
		Config            string
		Dir               string
		MaxSendSize       int64
		MaxFileSize       int64
		RotateIntervalSec int64
	}

	logFd *os.File
)

func init() {
	// actions
	flag.BoolVar(&Argv.help, `h`, false, `show this help`)
	flag.BoolVar(&Argv.version, `version`, false, `show version`)
	flag.BoolVar(&Argv.reverse, `reverse`, false, `start reverse proxy server instead (ch-addr is used as clickhouse host-port)`)

	// common options
	flag.StringVar(&Argv.Host, `host`, `0.0.0.0`, `listening host`)
	flag.UintVar(&Argv.Port, `port`, 13338, `listening port. REQUIRED`)
	flag.UintVar(&Argv.Port, `p`, 13338, `listening port. REQUIRED`)
	flag.StringVar(&Argv.User, `u`, `kitten`, "setuid user (if needed)")
	flag.StringVar(&Argv.Group, `g`, `kitten`, "setgid user (if needed)")
	flag.StringVar(&Argv.Log, `l`, "", "log file (if needed)")
	flag.StringVar(&Argv.ChHost, `ch-addr`, `127.0.0.1:8123`, `default clickhouse host:port`)
	flag.UintVar(&Argv.NProc, `cores`, uint(0), `max cpu cores usage`)
	flag.StringVar(&Argv.PprofHostPort, `pprof`, ``, `host:port for http pprof`)
	flag.Uint64Var(&Argv.MaxOpenFiles, `max-open-files`, 262144, `open files limit`)
	flag.StringVar(&Argv.ChDatabase, `db`, `default`, "clickhouse database")
	flag.StringVar(&Argv.ChUser, `ch-user`, ``, "clickhouse user")
	flag.StringVar(&Argv.ChPassword, `ch-password`, ``, "clickhouse password")
	flag.StringVar(&Argv.ChSslCertPath, `ch-ssl-cert-path`, ``, "clickhouse SSL cert path")

	// local proxy options
	flag.StringVar(&Argv.Config, `c`, ``, `path to routing config`)
	flag.StringVar(&Argv.Dir, `dir`, `/tmp/kittenhouse`, `dir for persistent logs`)
	flag.Int64Var(&Argv.MaxSendSize, `max-send-size`, 1<<20, `max batch size to be sent to clickhouse in bytes`)
	flag.Int64Var(&Argv.MaxFileSize, `max-file-size`, 50<<20, `max file size in bytes`)
	flag.Int64Var(&Argv.RotateIntervalSec, `rotate-interval-sec`, 1800, `how often to rotate files`)
	flag.BoolVar(&Argv.MarkAsDone, `mark-as-done`, false, `rename files to *.done instead of deleting them upon successful delivery`)

	flag.Parse()
}