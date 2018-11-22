package persist

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vkcom/kittenhouse/core/clickhouse"
	"github.com/vkcom/kittenhouse/core/destination"
)

const (
	offsetsFile     = "offsets.json"
	doneSuffix      = ".done"
	logSuffix       = ".clk"
	pollInterval    = 2 * time.Second
	maxPollInterval = pollInterval * 32
)

// each sender is single-threaded so no mutexes here
type sender struct {
	// settings (initialized once)
	isDefault bool
	dst       *destination.Setting
	tables    []string // set for non-default sender
	stopCh    chan chan struct{}
	filesCh   chan []string // channel for list of files to process

	// buffers
	buf        []byte // buffer of what is going to be sent to clickhouse
	readBuf    [4096]byte
	crcBuf     [8]byte
	curLine    []byte
	linesCount int64

	// state variables
	brokenFilesMap map[string]*brokenState // state of files with syntax errors, column count mismatches, etc
}

type brokenState struct {
	maxSendSize     int64
	brokenBytesLeft int64
}

var senderMap struct {
	sync.Mutex
	v map[destination.ServersStr]*sender
}

func getAcknowledgedOffset(relFilename string) ackOff {
	acknowlegedOffsetsMap.Lock()
	off := acknowlegedOffsetsMap.v[relFilename]
	acknowlegedOffsetsMap.Unlock()

	return off
}

func setAcknowledgedOffset(relFilename string, off ackOff) {
	acknowlegedOffsetsMap.Lock()
	acknowlegedOffsetsMap.v[relFilename] = off
	acknowlegedOffsetsMap.Unlock()
}

func initAcknowlegedOffsetsMapOrDie() {
	fp, err := os.Open(filepath.Join(conf.Dir, offsetsFile))
	if err != nil {
		if os.IsNotExist(err) {
			go flushAcknowlegedOffsetsMapThread()
			return
		}
		log.Fatalf("Could not read acknowledged offsets map: %s", err.Error())
	}
	defer fp.Close()

	acknowlegedOffsetsMap.Lock()
	defer acknowlegedOffsetsMap.Unlock()

	if err := json.NewDecoder(fp).Decode(&acknowlegedOffsetsMap.v); err != nil {
		log.Printf("Could not read acknowledged offsets map: %s", err.Error())
	}

	go flushAcknowlegedOffsetsMapThread()
}

func flushAcknowlegedOffsetsMapThread() {
	for {
		time.Sleep(time.Second)
		if err := FlushAcknowlegedOffsetsMap(); err != nil {
			log.Printf("Could not flush acknowledged offsets map: %s", err.Error())
		}
	}
}

func markFilesDone(ackOffsets map[string]ackOff) (toDelete []string) {
	for relFilename, off := range ackOffsets {
		filename := filepath.Join(conf.Dir, relFilename)

		st, err := os.Stat(filename)
		if err != nil {
			if os.IsNotExist(err) {
				toDelete = append(toDelete, relFilename)
			}
			continue
		}

		if st.Size() <= off.Offset && time.Now().Sub(st.ModTime()) > conf.RotateInterval+time.Second*10 && !strings.HasSuffix(filename, doneSuffix) {

			if conf.MarkAsDone {
				if err := os.Rename(filename, filename+doneSuffix); err != nil {
					log.Printf("Could not mark %s as done: %s", filename, err.Error())
					continue
				}
			} else {
				if err := os.Remove(filename); err != nil {
					log.Printf("Could not delete %s: %s", filename, err.Error())
					continue
				}
			}

			toDelete = append(toDelete, relFilename)
		}
	}

	return toDelete
}

var flushAckMutex sync.Mutex

// FlushAcknowlegedOffsetsMap saves current offsets map.
// It is made public so that offsets can be saved right before SIGTERM.
func FlushAcknowlegedOffsetsMap() error {
	flushAckMutex.Lock()
	defer flushAckMutex.Unlock()

	var b bytes.Buffer

	enc := json.NewEncoder(&b)
	enc.SetIndent("", " ")

	acknowlegedOffsetsMap.Lock()
	mapCopy := make(map[string]ackOff, len(acknowlegedOffsetsMap.v))
	for k, v := range acknowlegedOffsetsMap.v {
		mapCopy[k] = v
	}
	acknowlegedOffsetsMap.Unlock()

	// cleaning can take some time, so we do it without lock
	toDelete := markFilesDone(mapCopy)

	acknowlegedOffsetsMap.Lock()
	for _, relFilename := range toDelete {
		delete(acknowlegedOffsetsMap.v, relFilename)
	}
	err := enc.Encode(acknowlegedOffsetsMap.v)
	acknowlegedOffsetsMap.Unlock()

	if err != nil {
		log.Fatalf("Could not json encode acknowlegedOffsets: %s", err.Error())
	}

	tmpFile := filepath.Join(conf.Dir, offsetsFile+".tmp")
	if err := ioutil.WriteFile(tmpFile, b.Bytes(), 0666); err != nil {
		return err
	}

	return os.Rename(tmpFile, filepath.Join(conf.Dir, offsetsFile))
}

func readFilesThread() {
	for {
		readFilesIteration()
		time.Sleep(time.Second * 2)
	}
}

func readFilesIteration() {
	fis, err := ioutil.ReadDir(conf.Dir)
	if err != nil {
		log.Printf("Could not read %s: %s", conf.Dir, err.Error())
		return
	}

	senderMap.Lock()
	defer senderMap.Unlock()

	var defaultFiles []string
	var defaultCh chan []string
	filesMap := make(map[chan []string][]string)
	tableChMap := make(map[string]chan []string)

	for _, s := range senderMap.v {
		if s.isDefault {
			defaultCh = s.filesCh
			continue
		}

		for _, table := range s.tables {
			tableChMap[table] = s.filesCh
		}
	}

	for _, fi := range fis {
		relFilename := fi.Name()
		if fi.IsDir() || !strings.HasSuffix(relFilename, logSuffix) {
			continue
		}

		table := getTableFromFileName(relFilename)
		if ch, ok := tableChMap[table]; ok {
			filesMap[ch] = append(filesMap[ch], relFilename)
		} else {
			defaultFiles = append(defaultFiles, relFilename)
		}
	}

	for ch, filesList := range filesMap {
		select {
		case ch <- filesList:
		default:
		}
	}

	if defaultCh != nil {
		select {
		case defaultCh <- defaultFiles:
		default:
		}
	}
}

func (s *sender) loop() {
	var servers []string
	for _, srv := range s.dst.Servers {
		if srv.Weight > 0 {
			servers = append(servers, fmt.Sprintf("%s*%d", srv.HostPort, srv.Weight))
		} else {
			servers = append(servers, string(srv.HostPort))
		}
	}
	log.Printf("Starting persistent loop (servers = [%s], default = %v, tables = %+v)", strings.Join(servers, " "), s.isDefault, s.tables)

	sleepInterval := pollInterval

	for {
		select {
		case ch := <-s.stopCh:
			log.Printf("Stopped persistent loop (default = %v, tables = %+v)", s.isDefault, s.tables)
			ch <- struct{}{}
			return
		case filesList := <-s.filesCh:
			fullyDelivered, err := s.loopIteration(filesList)

			if err == nil {
				sleepInterval = pollInterval
			} else {
				log.Printf("Could not loop iteration: %s", err.Error())
				sleepInterval *= 2
				if sleepInterval > maxPollInterval {
					sleepInterval = maxPollInterval
				}
			}

			if !fullyDelivered && err == nil {
				// super ugly hack to force reiteration immediately
				select {
				case s.filesCh <- filesList:
				default:
				}
			} else {
				time.Sleep(sleepInterval)
			}
		}
	}
}

func (s *sender) loopIteration(filesList []string) (fullyDelivered bool, err error) {
	fullyDelivered = true

	for _, relFilename := range filesList {
		off := getAcknowledgedOffset(relFilename)
		st, err := os.Stat(filepath.Join(conf.Dir, relFilename))
		if err != nil {
			if !os.IsNotExist(err) {
				log.Printf("Could not stat %s: %s", relFilename, err.Error())
			}
			continue
		} else if off.Offset >= st.Size() {
			continue
		}

		off, fully, sendErr := s.sendFile(relFilename, off)
		if sendErr != nil {
			log.Printf("Could not send file %s: %s", relFilename, sendErr.Error())
			err = sendErr
		} else {
			if !fully {
				fullyDelivered = false
			}
			setAcknowledgedOffset(relFilename, off)
		}
	}

	return fullyDelivered, err
}

func (s *sender) sendFile(relFilename string, off ackOff) (newOff ackOff, fullyDelivered bool, err error) {
	fp, err := os.Open(filepath.Join(conf.Dir, relFilename))
	if err != nil {
		// FlushAcknowlegedOffsetsMap works in another goroutine and renames files to ".done", it is not an error
		if os.IsNotExist(err) {
			return off, false, nil
		}
		return off, false, err
	}
	defer fp.Close()

	fullyDelivered = true

	if off.Table == "" {
		// This should be a very rare case when daemon crashed before it could write new offsets file.
		// We still need to handle this situation gracefully.
		off.Table, off.RowBinary, err = s.determineTableName(fp)
		if err != nil {
			return off, false, err
		}
	}

	if _, err = fp.Seek(off.Offset, os.SEEK_SET); err != nil {
		return off, false, err
	}

	s.buf = s.buf[0:0]
	s.linesCount = 0

	maxSendSize := conf.MaxSendSize
	brk := s.brokenFilesMap[relFilename]
	if brk != nil {
		maxSendSize = brk.maxSendSize
	}

	bytesRead, fullyDelivered, err := s.readLinesIntoBuf(fp, maxSendSize, off.RowBinary)
	if err != nil {
		return off, false, err
	}

	if bytesRead <= 0 || len(s.buf) == 0 {
		return off, true, nil
	}

	if debug {
		log.Printf("Flushing %s (%d bytes)", off.Table, len(s.buf))
	}

	if err := clickhouse.Flush(s.dst, off.Table, s.buf, off.RowBinary); err != nil {
		shouldRetry := s.handleSyntaxErrors(relFilename, off.Table, bytesRead, err)
		if shouldRetry {
			return off, false, err
		}
	}

	off.Offset += bytesRead

	if brk != nil {
		brk.brokenBytesLeft -= bytesRead
		if brk.brokenBytesLeft <= 0 {
			delete(s.brokenFilesMap, relFilename)
		} else {
			brk.maxSendSize *= 2
			if brk.maxSendSize > conf.MaxSendSize {
				brk.maxSendSize = conf.MaxSendSize
			}
		}
	}

	return off, fullyDelivered, nil
}

// попытаться "обезвредить" синтаксические (и другие) ошибки в батче, чтобы можно было послать все остальное
func (s *sender) handleSyntaxErrors(relFilename, table string, bytesRead int64, err error) (shouldRetry bool) {
	if !clickhouse.IsSyntaxError(err) {
		return true
	}

	if table == internalBufferTable {
		return false // too bad that we have syntax error for internal buffer log, we cannot log it into itself
	}

	if s.linesCount <= 1 {
		InternalLog("persist.syntax_error", table, int64(len(s.buf)), err.Error(), string(s.buf))
		return false
	}

	brk, ok := s.brokenFilesMap[relFilename]

	if ok {
		brk.maxSendSize /= 2
	} else {
		s.brokenFilesMap[relFilename] = &brokenState{
			maxSendSize:     conf.MaxSendSize / 2,
			brokenBytesLeft: bytesRead,
		}
	}

	return true
}

var headerSep = []byte("#")

// determineTableName extracts header from file contents in case there is no header in offsets file
func (s *sender) determineTableName(fp io.Reader) (tableName string, rowBinary bool, err error) {
	r := bufio.NewReader(fp)

	for {
		ln, err := r.ReadBytes('\n')
		if err != nil {
			return "", false, err
		}

		ln, ok := s.trimCrc(ln[0 : len(ln)-1])
		if !ok || len(ln) <= 1 || ln[0] != '#' {
			continue
		}

		parts := bytes.SplitN(ln, headerSep, 4)
		if len(parts) < 3 {
			log.Printf("Unrecognized header (got less than 2 '#' chars): '%s'", ln)
			continue
		}

		if len(parts) == 4 && strings.TrimSpace(string(parts[3])) == "RowBinary" {
			rowBinary = true
		}

		return strings.TrimSpace(string(parts[2])), rowBinary, nil
	}
}

func (s *sender) trimCrc(ln []byte) (cleanLn []byte, ok bool) {
	if len(ln) < 8 {
		log.Printf("Got broken line (len < 8): '%s'", ln)
		return
	}

	crcStr := ln[len(ln)-8:]
	ln = ln[0 : len(ln)-8]

	actualCrc := appendCrcHex(s.crcBuf[0:0], crc32.Update(0, crc32.IEEETable, ln))

	if !bytes.Equal(actualCrc, crcStr) {
		log.Printf("Got broken line (invalid CRC32, expected '%s', got '%s'): '%s'", crcStr, actualCrc, ln)
		return nil, false
	}

	return ln, true
}

func (s *sender) writeLineIntoBuf(ln []byte, rowBinary bool) {
	if len(ln) > 0 && ln[0] == '#' { // ignore header
		return
	}

	ln, ok := s.trimCrc(ln)
	if !ok {
		return
	}

	s.linesCount++

	if !rowBinary && len(s.buf) > 0 {
		s.buf = append(s.buf, ',')
	}

	isEscape := false
	for _, c := range ln {
		if isEscape {
			if c == 'n' {
				s.buf = append(s.buf, '\n')
			} else { // if c == '\\'
				s.buf = append(s.buf, c)
			}
			isEscape = false
			continue
		}

		if c == '\\' {
			isEscape = true
			continue
		}

		s.buf = append(s.buf, c)
	}
}

// TODO: handle broken files that do not have last "\n" at the end (e.g. just ignore the last part if file was not modified for a long time)
func (s *sender) readLinesIntoBuf(r io.Reader, maxSendSize int64, rowBinary bool) (bytesRead int64, fullyDelivered bool, err error) {
	s.curLine = s.curLine[0:0]

	for {
		n, err := r.Read(s.readBuf[:])
		if n <= 0 {
			if err == io.EOF {
				return bytesRead, true, nil
			}
			return 0, false, err
		}

		for _, c := range s.readBuf[0:n] {
			if c == '\n' {
				s.writeLineIntoBuf(s.curLine, rowBinary)
				bytesRead += int64(len(s.curLine) + 1) // +1 here is because we do not put '\n' into curLine

				if bytesRead >= maxSendSize {
					return bytesRead, false, nil
				}

				s.curLine = s.curLine[0:0]
				continue
			}

			s.curLine = append(s.curLine, c)
		}
	}
}
