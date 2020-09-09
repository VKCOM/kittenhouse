package clickhouse

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/x509"
	"crypto/tls"

	"github.com/vkcom/engine-go/srvfunc"
	"github.com/vkcom/kittenhouse/core/cmdconfig"
	"github.com/vkcom/kittenhouse/core/destination"
)

const (
	kittenMeowCheckInterval    = time.Hour
	kittenMeowIdleTimeout      = 30 * time.Second
	kittenMeowHandshakeTimeout = 5 * time.Second
	maxExpectedResponseLength  = 16 << 10
)

var (
    // debug enable
	debug = (os.Getenv("CLICKHOUSE_DEBUG") != "")

    // database name for queries
	//databaseName = getEnv("CLICKHOUSE_DATABASE_NAME", "default");

	// database user
	//clickhouseUser = getEnv("CLICKHOUSE_USER", "");

    // database password
    //clickhousePassword = getEnv("CLICKHOUSE_PASSWORD", "");

	// kittenMeow is a custom protocol over HTTP that allows to efficiently stream lots of data
	kittenMeowConn = struct {
		sync.Mutex
		m map[destination.ServerHostPort]*kittenMeow
	}{
		m: make(map[destination.ServerHostPort]*kittenMeow),
	}
    // generate HTTP client
	httpClient = generateHttpClient()
)

type kittenMeow struct {
	sync.Mutex
	supported   bool
	lastChecked time.Time
	conn        net.Conn
	lastQueryTs time.Time
}

type httpError struct {
	method string
	url    string
	code   int
	table  string
	msg    string
}

// Init starts off useful goroutines
func Init() {
	go closeIdleKittenMeows()
}

func closeIdleKittenMeows() {
	for {
		time.Sleep(time.Second * 5)

		conns := make(map[destination.ServerHostPort]*kittenMeow)

		kittenMeowConn.Lock()
		for srv, c := range kittenMeowConn.m {
			conns[srv] = c
		}
		kittenMeowConn.Unlock()

		for srv, c := range conns {
			c.Lock()
			if c.conn != nil && time.Now().Sub(c.lastQueryTs) > kittenMeowIdleTimeout {
				if debug {
					log.Printf("Closing idle connection to %s", srv)
				}

				c.conn.Close()
				c.conn = nil
			}
			c.Unlock()
		}
	}
}

func (e *httpError) Error() string {
	return fmt.Sprintf("%s %s: ClickHouse server returned HTTP code %d: %s", e.method, e.url, e.code, e.msg)
}

// IsSyntaxError returns whether or not error was caused by wrong syntax in a query
func IsSyntaxError(err error) bool {
	if err == nil {
		return false
	}

	e, ok := err.(*httpError)
	if !ok {
		return false
	}

	return strings.Contains(e.msg, `Cannot parse input:`) || strings.Contains(e.msg, `Type mismatch`) || strings.Contains(e.msg, `Cannot read all data`)
}

// Flush sends data to clickhouse and returns errors if any
func Flush(dst *destination.Setting, table string, body []byte, rowBinary bool) error {
	if strings.Contains(table, "@") {
		// table name contains shard, we need to cut it:
		// table_name@shard(col1, ..., colN) -> table_name(col1, ..., colN)
		parts := strings.SplitN(table, "(", 2)
		if len(parts) == 2 {
			tbl := parts[0]
			cols := parts[1]
			tblParts := strings.SplitN(tbl, "@", 2)
			if len(tblParts) == 2 {
				table = tblParts[0] + "(" + cols
			}
		}
	}

	/*
		if err := flush(srv, table, body, rowBinary, true); err != nil {
			log.Printf("Could not post to table %s to clickhouse with compression: %s", table, err.Error())
			return flush(srv, table, body, rowBinary, false)
		}
		return nil
	*/
	return flush(dst, table, body, rowBinary, false)
}

// We assume that there will not be infinitely many different servers so there is no need to
// clear the map
func getKittenMeowForServer(srv destination.ServerHostPort) *kittenMeow {
	kittenMeowConn.Lock()
	defer kittenMeowConn.Unlock()

	el, ok := kittenMeowConn.m[srv]
	if !ok {
		el = &kittenMeow{}
		kittenMeowConn.m[srv] = el
	}

	return el
}

func checkHostAlive(srv destination.ServerHostPort) error {
	code, res, err := queryServer(time.Now().Add(time.Minute), srv, `SELECT 42 FORMAT TabSeparated`)
	if err != nil {
		return err
	}

	if code != http.StatusOK {
		return fmt.Errorf("Got HTTP status %d instead of %d: %s", code, http.StatusOK, res)
	}

	if strings.TrimSpace(string(res)) != `42` {
		return fmt.Errorf("Got unexpected result for 'SELECT 42': %s", res)
	}

	return nil
}

var nginxBytes = []byte("nginx")

func insertQueryPrefixEscaped(table string, rowBinary bool) string {
	if rowBinary {
		return url.PathEscape(fmt.Sprintf("INSERT INTO %s FORMAT RowBinary", table))
	}

	return url.PathEscape(fmt.Sprintf("INSERT INTO %s VALUES", table))
}

func (m *kittenMeow) tryFlush(srv destination.ServerHostPort, table string, body []byte, rowBinary bool, compression bool) (supported bool, err error) {
	start := time.Now()

	if !m.supported && time.Now().Sub(m.lastChecked) > kittenMeowCheckInterval {
		m.lastChecked = time.Now()
		if err := m.connect(srv); err != nil {
			return false, err
		}
	}

	if m.supported && m.conn == nil {
		if err := m.connect(srv); err != nil {
			return false, err
		}
	}

	if !m.supported {
		return false, nil
	}

	m.conn.SetWriteDeadline(time.Now().Add(time.Minute))

	wr := bufio.NewWriter(m.conn)

	lenBuf := make([]byte, 4)

	lenBuf[0] = 0
	if rowBinary {
		lenBuf[0] |= FlagRowBinary
	}
	if err := m.writeOrClose(wr, lenBuf[0:1]); err != nil {
		return true, err
	}

	binary.LittleEndian.PutUint16(lenBuf[0:2], uint16(len(table)))
	if err := m.writeOrClose(wr, lenBuf[0:2]); err != nil {
		return true, err
	}

	if err := m.writeStringOrClose(wr, table); err != nil {
		return true, err
	}

	binary.LittleEndian.PutUint32(lenBuf[0:4], uint32(len(body)))
	if err := m.writeOrClose(wr, lenBuf[0:4]); err != nil {
		return true, err
	}

	if err := m.writeOrClose(wr, body); err != nil {
		return true, err
	}

	if err := m.flushOrClose(wr); err != nil {
		return true, err
	}

	m.conn.SetReadDeadline(time.Now().Add(time.Minute))
	rd := bufio.NewReader(m.conn)

	// read and ignore flags byte
	if err := m.readOrClose(rd, lenBuf[0:1]); err != nil {
		return true, err
	}

	// response code
	if err := m.readOrClose(rd, lenBuf[0:4]); err != nil {
		return true, err
	}

	httpCode := binary.LittleEndian.Uint32(lenBuf[0:4])

	// response text length
	if err := m.readOrClose(rd, lenBuf[0:4]); err != nil {
		return true, err
	}

	respLen := binary.LittleEndian.Uint32(lenBuf[0:4])
	if respLen > maxExpectedResponseLength {
		log.Printf("Server '%s' wanted to send response for INSERT of %d bytes (max %d), closing connection", srv, respLen, maxExpectedResponseLength)

		return true, ErrTooBigResponse
	}

	respBody := make([]byte, respLen)
	if err := m.readOrClose(rd, respBody); err != nil {
		return true, err
	}

	m.lastQueryTs = time.Now()
	if httpCode != http.StatusOK {
		log.Printf("Could not post to table %s to clickhouse (HTTP code %d): %s", table, httpCode, respBody)
		return true, &httpError{
			method: "KITTEN",
			url:    fmt.Sprintf("http+kitten://%s/", srv),
			code:   int(httpCode),
			msg:    string(respBody),
			table:  table,
		}
	}

	if debug {
		log.Printf("Sent %d KiB using KITTEN/MEOW to %s for %s", len(body)/1024, srv, time.Since(start))
	}

	return true, nil
}

func (m *kittenMeow) readOrClose(rd *bufio.Reader, buf []byte) error {
	_, err := io.ReadFull(rd, buf)
	if err != nil {
		m.conn.Close()
		m.conn = nil
	}
	return err
}

func (m *kittenMeow) flushOrClose(wr *bufio.Writer) error {
	err := wr.Flush()
	if err != nil {
		m.conn.Close()
		m.conn = nil
	}
	return err
}

func (m *kittenMeow) writeOrClose(wr *bufio.Writer, buf []byte) error {
	_, err := wr.Write(buf)
	if err != nil {
		m.conn.Close()
		m.conn = nil
	}
	return err
}

func (m *kittenMeow) writeStringOrClose(wr *bufio.Writer, s string) error {
	_, err := wr.WriteString(s)
	if err != nil {
		m.conn.Close()
		m.conn = nil
	}
	return err
}

func (m *kittenMeow) connect(srv destination.ServerHostPort) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	m.conn, err = srvfunc.CachingDialer(ctx, "tcp", string(srv))
	if err != nil {
		return err
	}

	err = m.initConn(srv)
	if !m.supported && m.conn != nil {
		m.conn.Close()
		m.conn = nil
	}

	return err
}

var meowBytes = []byte("MEOW")

func (m *kittenMeow) initConn(srv destination.ServerHostPort) (err error) {
	m.supported = false
	m.conn.SetWriteDeadline(time.Now().Add(kittenMeowHandshakeTimeout))
	m.conn.SetReadDeadline(time.Now().Add(kittenMeowHandshakeTimeout))

	_, err = io.WriteString(m.conn, "KITTEN / HTTP/1.1\n\n")
	if err != nil {
		return err
	}

	rd := bufio.NewReader(m.conn)
	got200 := false

	for {
		ln, err := rd.ReadString('\n')
		if err != nil {
			if debug {
				log.Printf("Error while reading headers from '%s': %s", srv, err.Error())
			}

			return err
		}

		ln = strings.TrimSpace(ln)

		if !got200 {
			if ln != `HTTP/1.1 200 OK` {
				if debug {
					log.Printf("Server %s responded with '%s' to KITTEN", srv, ln)
				}
				return nil
			}

			got200 = true
		}

		const contentLengthHeader = "Content-Length: "
		if strings.HasPrefix(ln, contentLengthHeader) {
			contentLen, err := strconv.Atoi(strings.TrimPrefix(ln, contentLengthHeader))
			if err != nil {
				if debug {
					log.Printf("Server %s sent invalid Content-Length header '%s': %s", srv, ln, err.Error())
				}
				return err
			}

			if contentLen != len(meowBytes) {
				if debug {
					log.Printf("Server %s sent Content-Length of %d, expected to get %d", srv, contentLen, len(meowBytes))
				}
				return nil
			}
		}

		// end of headers
		if ln == "" {
			break
		}
	}

	meowBuf := make([]byte, len(meowBytes))
	_, err = io.ReadFull(rd, meowBuf)
	if err != nil {
		return err
	}

	if !bytes.Equal(meowBuf, meowBytes) {
		if debug {
			log.Printf("Server sent '%s' instead of MEOW", meowBytes)
		}
		return nil
	}

	if debug {
		log.Printf("Server %s supports KITTEN/MEOW", srv)
	}

	m.supported = true
	return nil
}

func flush(dst *destination.Setting, table string, body []byte, rowBinary bool, compression bool) error {
	srv, ok := dst.ChooseNextServer()
	if !ok {
		return ErrTemporarilyUnavailable
	}

	// do not estabilish more than a single HTTP connection to clickhouse server
	meow := getKittenMeowForServer(srv)

	meow.Lock()
	defer meow.Unlock()

	supported, err := meow.tryFlush(srv, table, body, rowBinary, compression)
	if supported {
		if err != nil {
			log.Printf("Could not KITTEN/MEOW to table %s to clickhouse: %s", table, err.Error())

			// see comment below about why we only check for network errors, not HTTP status codes
			if _, ok := err.(*httpError); !ok {
				dst.TempDisableHost(srv, checkHostAlive)
			}
		}
		return err
	}

	// compression is not too stable
	if compression {
		body = compress(body)
	}

	start := time.Now()

	queryPrefix := insertQueryPrefixEscaped(table, rowBinary)

	compressionArgs := ""
	if compression {
		compressionArgs = "decompress=1&http_native_compression_disable_checksumming_on_decompress=1&"
	}

	url := fmt.Sprintf("http://%s/?input_format_values_interpret_expressions=0&%squery=%s&database=%s", srv, compressionArgs, queryPrefix, cmd_config.argv.chDatabase)

	//resp, err := httpClient.Post(url, "application/x-www-form-urlencoded", bytes.NewReader(body))
	// generate request
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
	    panic(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	// add clickhouse user
	if cmd_config.argv.chUser != "" {
	    req.Header.Add("X-ClickHouse-User", cmd_config.argv.chUser)
	}
	// add clickhouse password
	if cmd_config.argv.chPassword != "" {
	    req.Header.Add("X-ClickHouse-Key", cmd_config.argv.chPassword)
	}
	// send request
	resp, err := httpClient.Do(req)
    // check error
	if err != nil {
		log.Printf("Could not post to table %s to clickhouse: %s", table, err.Error())
		dst.TempDisableHost(srv, checkHostAlive)
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyText, _ := ioutil.ReadAll(resp.Body)
		log.Printf("Could not post to table %s to clickhouse (HTTP code %d): %s", table, resp.StatusCode, bodyText)
		// Can only be sure that server is misconfigured if nginx responds instead of ClickHouse.
		// Other problems like 404 error may indicate temporary problems like dropped buffer table.
		// Even if we got 500 error upon inserting to table it does not mean that server is down, it can be a
		// problem with concrete table.
		if bytes.Contains(bodyText, nginxBytes) {
			dst.TempDisableHost(srv, checkHostAlive)
		}
		// log.Fatalf("Failed query: %s", body)
		return &httpError{
			method: "POST",
			url:    url,
			code:   resp.StatusCode,
			msg:    string(bodyText),
			table:  table,
		}
	}

	io.Copy(ioutil.Discard, resp.Body) // keepalive

	if debug {
		log.Printf("Sent %d KiB to clickhouse server %s for %s", len(body)/1024, srv, time.Since(start))
	}

	return nil
}

// generate HTTP client
func generateHttpClient() *http.Client {
    // SSL cert path
    //var sslCertPath = getEnv("CLICKHOUSE_SSL_CERT_PATH", "")
    var sslCertPath = cmd_config.argv.chSslCertPath
    //
    if sslCertPath != "" {
        // read cert
        caCert, err := ioutil.ReadFile(sslCertPath)
        // if cert absent, panic
        if err != nil { panic(err) }
        var caCertPool = x509.NewCertPool()
        caCertPool.AppendCertsFromPEM(caCert)
        return &http.Client{
                    Transport: &http.Transport{
               		MaxIdleConnsPerHost: 1,
               		DialContext:         srvfunc.CachingDialer,
               		TLSClientConfig: &tls.Config{
               		    RootCAs: caCertPool,
               		},
                },
            Timeout: time.Minute,
        }
    } else {
        return &http.Client{
                    Transport: &http.Transport{
               		MaxIdleConnsPerHost: 1,
               		DialContext:         srvfunc.CachingDialer,
                },
            Timeout: time.Minute,
        }
    }
}

// Simple helper function to read an environment or return a default value
func getEnv(key string, defaultVal string) string {
    if value, exists := os.LookupEnv(key); exists {
	return value
    }
    return defaultVal
}