package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Tirael666/engine-go/srvfunc"
	"github.com/vkcom/kittenhouse/core/destination"
)

const (
	defaultTable  = ""
	maxResultSize = 1000000 // a little less than 1 MiB so that we can add headers and stuff to response
)

var (
	// ErrMustBeSelect means that you sent something that is probably not a SELECT query.
	// We do not support other queries because we do not know where to send query without a table name.
	ErrMustBeSelect = errors.New("Query must be in form 'SELECT ... FROM table'")

	// ErrTooBigResult means that daemon would probably exceed maximum response size if full result is sent.
	ErrTooBigResult = fmt.Errorf("Result exceeds %d MiB", maxResultSize/(1<<20))

	// ErrTooBigResponse means that ClickHouse server was going to send too big of a response to INSERT
	ErrTooBigResponse = fmt.Errorf("Too big response from server")

	// ErrTemporarilyUnavailable means that there are no servers alive that have the requested table
	ErrTemporarilyUnavailable = errors.New("No servers available")

	tableDstMap struct {
		sync.RWMutex
		m map[string]*destination.Setting
	}
	queryRegex      = regexp.MustCompile(`(?s)(?:SELECT|select)\s.*?(?:FROM|from)\s+([a-zA-Z_][a-zA-Z0-9_.]*)`)
	queryHTTPClient = &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1,
			DialContext:         srvfunc.CachingDialer,
		},
	}
)

func maybeAddTable(m map[string]*destination.Setting, table string, dst *destination.Setting) {
	if _, ok := m[table]; !ok {
		m[table] = dst
	}
}

// Escape escapes string for MySQL. It should work for ClickHouse as well.
func Escape(txt string) string {
	var (
		esc string
		buf bytes.Buffer
	)
	last := 0
	for ii, bb := range txt {
		switch bb {
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\\':
			esc = `\\`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\032':
			esc = `\Z`
		default:
			continue
		}
		io.WriteString(&buf, txt[last:ii])
		io.WriteString(&buf, esc)
		last = ii + 1
	}
	io.WriteString(&buf, txt[last:])
	return buf.String()
}

// UpdateDestinationsConfig is used for QueryDeadline to determine where to send query.
func UpdateDestinationsConfig(m destination.Map) {
	tableDstMap.Lock()
	defer tableDstMap.Unlock()

	tableDstMap.m = make(map[string]*destination.Setting)

	for _, settings := range m {
		if settings.Default {
			tableDstMap.m[defaultTable] = settings
			continue
		}

		for _, tbl := range settings.Tables {
			tableDstMap.m[tbl] = settings
			maybeAddTable(tableDstMap.m, strings.TrimSuffix(tbl, "_buffer"), settings)
			if !strings.Contains(tbl, ".") {
				maybeAddTable(tableDstMap.m, "default."+tbl, settings)
				maybeAddTable(tableDstMap.m, "default."+strings.TrimSuffix(tbl, "_buffer"), settings)
			}
		}
	}
}

// GetDestinationSetting returns setting for specified table name
func GetDestinationSetting(tableName string) *destination.Setting {
	tableDstMap.RLock()
	dst, ok := tableDstMap.m[tableName]
	if !ok {
		dst = tableDstMap.m[defaultTable]
	}
	tableDstMap.RUnlock()
	return dst
}

// QueryDeadline performs supplied query at appropriate server.
// There is no limit on number of concurrent queries, so use this method with care.
func QueryDeadline(deadline time.Time, query string) (httpCode int, res []byte, err error) {
	if strings.HasPrefix(query, chunkQuery) {
		id, err := strconv.Atoi(strings.TrimPrefix(query, chunkQuery))
		if err != nil {
			return 0, nil, err
		}

		buf, eof, err := getNextChunk(int32(id))
		if err != nil {
			return 0, nil, err
		}

		if eof {
			return http.StatusNoContent, nil, nil
		}

		return http.StatusPartialContent, buf, err
	}

	matches := queryRegex.FindStringSubmatch(query)
	if len(matches) == 0 {
		return 0, nil, ErrMustBeSelect
	}

	tableName := matches[1]
	dst, ok := GetDestinationSetting(tableName).ChooseNextServer()
	if !ok {
		return 0, nil, ErrTemporarilyUnavailable
	}

	return queryServer(deadline, dst, query)
}

const chunkQuery = "GET NEXT CHUNK FOR "

func queryServer(deadline time.Time, dst destination.ServerHostPort, query string) (httpCode int, res []byte, err error) {
	maxExecSec := deadline.Sub(time.Now())/time.Second + 1

	asyncResult := false
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer func() {
		if !asyncResult {
			cancel()
		}
	}()

	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/?max_execution_time=%d&query=%s", dst, maxExecSec, url.QueryEscape(query)), nil)
	if err != nil {
		return 0, nil, err
	}

	resp, err := queryHTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return 0, nil, err
	}

	defer func() {
		if !asyncResult {
			resp.Body.Close()
		}
	}()

	rd := &io.LimitedReader{
		R: resp.Body,
		N: maxResultSize,
	}

	res, err = ioutil.ReadAll(rd)
	if err != nil {
		return 0, nil, err
	}

	if rd.N > 0 {
		return resp.StatusCode, res, nil
	}

	asyncResult = true
	id := addPendingRequest(cancel, resp.Body, res)

	return http.StatusTeapot, []byte(fmt.Sprintf("PENDING REQUEST ID %d", id)), nil
}
