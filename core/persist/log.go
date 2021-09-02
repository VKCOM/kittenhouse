package persist

import (
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vkcom/Tirael666/core/clickhouse"
	"github.com/vkcom/Tirael666/core/kittenerror"
)

const (
	debug = false

	ErrCodeFileOpen  = 100
	ErrCodeFileWrite = 101
)

type (
	tableFp struct {
		filename string

		mu           sync.Mutex // local mutex instead of flock for concurrent writes
		createdTs    time.Time
		bytesWritten int64
		writeFp      *os.File
	}

	ackOff struct {
		Table     string `json:"table"`
		Offset    int64  `json:"offset"`
		RowBinary bool   `json:"row_binary,omitempty"`
	}
)

var (
	filesMap = struct {
		sync.RWMutex
		v map[string]*tableFp
	}{
		v: make(map[string]*tableFp),
	}

	acknowlegedOffsetsMap = struct {
		sync.Mutex
		v map[string]ackOff
	}{
		v: make(map[string]ackOff),
	}

	globalLockFp *os.File
)

func checkBinLogDirOrDie() {
	if _, err := os.Stat(conf.Dir); err != nil {
		log.Fatalf("Bad dir for binlog: %s", err.Error())
	}

	var err error
	globalLockFp, err = os.OpenFile(filepath.Join(conf.Dir, "run.lock"), os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Could not create lock file: %s", err.Error())
	}

	if err := syscall.Flock(int(globalLockFp.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		log.Fatalf("Another instance of clickhouse proxy is already running in %s (%s)", conf.Dir, err.Error())
	}
}

const dtFormat = "20060102_150405"

func getTableFileName(table string, rowBinary bool) string {
	parts := strings.SplitN(table, "(", 2)
	if len(parts) == 1 {
		return parts[0]
	}

	dt := time.Now().In(time.Local).Format(dtFormat)

	h := md5.New()
	h.Write([]byte(parts[1]))
	if rowBinary {
		h.Write([]byte("|RowBinary"))
	}

	return fmt.Sprintf("%s_%s_%x%s", strings.TrimSpace(parts[0]), dt, h.Sum(nil)[0:4], logSuffix)
}

// gets table name (without columns list) from file name, assuming that filename has logSuffix
// and was formed using getTableFileName()
func getTableFromFileName(filename string) string {
	newLen := len(filename) - len(dtFormat) - len(logSuffix) - 2 /*underscores*/ - 8 /*md5 part*/
	if newLen <= 0 {
		return ""
	}
	return filename[0:newLen]
}

func writeHeader(fp *os.File, table string, rowBinary bool) error {
	hdr := fmt.Sprintf("# started at %s # %s", time.Now().Format("02-Jan-06 15:04:05"), table)

	if rowBinary {
		hdr += " # RowBinary"
	}

	buf := make([]byte, 0, len(hdr)+9) // hex + '\n'
	buf = append(buf, hdr...)
	buf = appendCrcHex(buf, crc32.Update(0, crc32.IEEETable, buf))
	buf = append(buf, '\n')

	_, err := fp.Write(buf)
	return err
}

func needRotate(tfp *tableFp) bool {
	return tfp.bytesWritten >= conf.MaxFileSize || time.Since(tfp.createdTs) >= conf.RotateInterval
}

func getFileForTable(table string, rowBinary bool) (*tableFp, error) {
	filesMap.RLock()
	tfp, ok := filesMap.v[table]
	filesMap.RUnlock()
	if ok && !needRotate(tfp) {
		return tfp, nil
	}

	filesMap.Lock()
	defer filesMap.Unlock()

	tfp, ok = filesMap.v[table]
	if ok && !needRotate(tfp) {
		return tfp, nil
	}

	relFilename := getTableFileName(table, rowBinary)
	filename := filepath.Join(conf.Dir, relFilename)

	if err := os.MkdirAll(filepath.Dir(filename), 0777); err != nil {
		return nil, err
	}

	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	tfp = &tableFp{
		writeFp:   fp,
		filename:  filename,
		createdTs: time.Now(),
	}

	if st, err := fp.Stat(); err == nil {
		tfp.bytesWritten = st.Size()
	}

	filesMap.v[table] = tfp

	if err := writeHeader(fp, table, rowBinary); err != nil {
		return nil, err
	}

	off := getAcknowledgedOffset(relFilename)
	off.Table = table
	off.RowBinary = rowBinary
	setAcknowledgedOffset(relFilename, off)

	return tfp, nil
}

func appendCrcHex(buf []byte, crc uint32) []byte {
	var tmp [8]byte
	for i := 0; i < 8; i++ {
		b := (crc & 0xF)
		if b >= 10 {
			b += 'A' - 10
		} else {
			b += '0'
		}
		tmp[len(tmp)-1-i] = byte(b)
		crc >>= 4
	}
	return append(buf, tmp[:]...)
}

// Write writes to per-table on-disk log and returns after data was put into filesystem.
func Write(table string, data []byte, rowBinary bool) error {
	tfp, err := getFileForTable(table, rowBinary)
	if err != nil {
		return kittenerror.NewCustom(ErrCodeFileOpen, "Could not open file with table contents", err.Error())
	}

	// TODO: think about pool.Get
	buf := make([]byte, 0, len(data)+10)

	for _, c := range data {
		if c == '\\' {
			buf = append(buf, '\\', '\\')
		} else if c == '\n' {
			buf = append(buf, '\\', 'n')
		} else {
			buf = append(buf, c)
		}
	}

	// write text representation of crc instead of binary so that we can actually use ReadString('\n') to read lines in file
	buf = appendCrcHex(buf, crc32.Update(0, crc32.IEEETable, buf))
	buf = append(buf, '\n')

	tfp.mu.Lock()
	defer tfp.mu.Unlock()

	n, err := tfp.writeFp.Write(buf)
	tfp.bytesWritten += int64(n)

	if err != nil {
		return kittenerror.NewCustom(ErrCodeFileWrite, "Could not write to file with table contents", err.Error())
	}

	return nil
}

const (
	internalBufferTable  = "internal_logs_buffer(time,server,port,type,table,volume,message,content)"
	heartbeatBufferTable = "daemon_heartbeat_buffer(time,daemon,server,port,build_version,build_commit,config_ts,config_hash,memory_used_bytes,cpu_user_avg,cpu_sys_avg)"
)

// InternalLog adds entry to internal log table.
// Be careful to avoid infinite recursion when using inside of persist package itself.
func InternalLog(typ, table string, volume int64, message, content string) {
	Write(internalBufferTable, []byte(fmt.Sprintf(
		"('%010d','%s',%d,'%s','%s',%d,'%s','%s')",
		time.Now().Unix(), clickhouse.Escape(conf.host), conf.Port, clickhouse.Escape(typ), clickhouse.Escape(table), volume, clickhouse.Escape(message), clickhouse.Escape(content),
	)), false)
}

// Heartbeat adds entry to daemon_heartbeat_buffer table.
func Heartbeat(buildVersion, buildCommit string, configTs int32, configHash string, memoryUsedBytes uint64, cpuUserAvg, cpuSysAvg float32) {
	Write(heartbeatBufferTable, []byte(fmt.Sprintf(
		"('%010d','kittenhouse','%s',%d,'%s','%s','%010d','%s',%d,%v,%v)",
		time.Now().Unix(), clickhouse.Escape(conf.host), conf.Port, clickhouse.Escape(buildVersion), clickhouse.Escape(buildCommit), configTs, clickhouse.Escape(configHash), memoryUsedBytes, cpuUserAvg, cpuSysAvg,
	)), false)
}
