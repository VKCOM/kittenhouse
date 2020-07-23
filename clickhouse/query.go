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

	"github.com/vkcom/kittenhouse/destination"
	"github.com/vkcom/engine-go/srvfunc"
)

const (
	defaultTable  = ""
	maxResultSize = 1000000 // a little less than 1 MiB so that we can add headers and stuff to response
)

type (
	// QueryParams for https://github.com/yandex/ClickHouse/blob/master/docs/ru/operations/settings/query_complexity.md
	// Check documentation https://clickhouse.yandex/docs/ru/interfaces/http/
	QueryParams struct {
		MaxExecutionSpeed      int32
		MaxExecutionSpeedBytes int32
		SessionId              string
		SessionTimeout         int32
		MaxResultRows          int32
		MaxExecutionTime       int32
		MaxColumnsToRead       int32
	}

	dstSessionIdItem struct {
		serverHost      destination.ServerHostPort
		sessionDeadline time.Time
	}
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

	tableDstSessionIdsMap struct {
		sync.RWMutex
		nextGc time.Time
		m      map[string]dstSessionIdItem
	}

	queryRegex      = regexp.MustCompile(`(?s)(?:SELECT|select)\s.*?(?:FROM|from)\s+([a-zA-Z_][a-zA-Z0-9_.]*(?:@[0-9]+)?)`)
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

func GetDestinationHostBySessionId(tableName string, params *QueryParams) (srv destination.ServerHostPort, ok bool) {
	const gcEvery = 5 * time.Minute

	// GC
	if tableDstSessionIdsMap.nextGc.IsZero() { // возможен рейс
		tableDstSessionIdsMap.Lock()
		if tableDstSessionIdsMap.nextGc.IsZero() {
			tableDstSessionIdsMap.nextGc = time.Now().Add(gcEvery)
		}
		tableDstSessionIdsMap.Unlock()
	} else if now := time.Now(); tableDstSessionIdsMap.nextGc.Before(now) { // возможен рейс
		tableDstSessionIdsMap.Lock()
		if tableDstSessionIdsMap.nextGc.Before(now) {
			tableDstSessionIdsMap.nextGc = now.Add(gcEvery)

			for sessionId, item := range tableDstSessionIdsMap.m {
				if item.sessionDeadline.Before(now) {
					delete(tableDstSessionIdsMap.m, sessionId)
				}
			}
		}
		tableDstSessionIdsMap.Unlock()
	}

	// Main section

	sessionId := params.SessionId

	sessionTimeout := time.Duration(params.SessionTimeout) * time.Second
	if sessionTimeout == 0 {
		sessionTimeout = 30 * time.Minute
	}

	tableDstSessionIdsMap.RLock()
	if dst, ok := tableDstSessionIdsMap.m[sessionId]; ok {
		srv = dst.serverHost
		dst.sessionDeadline = time.Now().Add(sessionTimeout)
		tableDstSessionIdsMap.m[sessionId] = dst
		tableDstSessionIdsMap.RUnlock()
		return srv, true
	}
	tableDstSessionIdsMap.RUnlock()

	tableDstSessionIdsMap.Lock()
	if dst, ok := tableDstSessionIdsMap.m[sessionId]; ok {
		srv = dst.serverHost
		dst.sessionDeadline = time.Now().Add(sessionTimeout)
		tableDstSessionIdsMap.m[sessionId] = dst
		tableDstSessionIdsMap.Unlock()
		return srv, true
	}

	srv, ok = GetDestinationSetting(tableName).ChooseNextServer()
	if !ok {
		tableDstSessionIdsMap.Unlock()
		return ``, false
	}

	if tableDstSessionIdsMap.m == nil {
		tableDstSessionIdsMap.m = make(map[string]dstSessionIdItem)
	}

	tableDstSessionIdsMap.m[sessionId] = dstSessionIdItem{
		serverHost:      srv,
		sessionDeadline: time.Now().Add(sessionTimeout),
	}

	tableDstSessionIdsMap.Unlock()
	return srv, true
}

// QueryDeadline performs supplied query at appropriate server.
// There is no limit on number of concurrent queries, so use this method with care.
func QueryDeadline(deadline time.Time, query string, params *QueryParams) (httpCode int, res []byte, err error) {
	const chunkQuery = "GET NEXT CHUNK FOR "

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

	var srv destination.ServerHostPort
	var ok bool

	if len(params.SessionId) > 0 {
		srv, ok = GetDestinationHostBySessionId(tableName, params)
	} else {
		srv, ok = GetDestinationSetting(tableName).ChooseNextServer()
	}

	if !ok {
		return 0, nil, ErrTemporarilyUnavailable
	}

	return queryServer(deadline, srv, query, params)
}

func buildUrlParams(maxExecSec time.Duration, params *QueryParams) string {
	urlParams := url.Values{}

	urlParams.Set(`max_execution_time`, strconv.Itoa(int(maxExecSec)))

	if params.MaxExecutionSpeed > 0 {
		urlParams.Set(`max_execution_speed`, strconv.Itoa(int(params.MaxExecutionSpeed)))
	}
	if params.MaxExecutionSpeedBytes > 0 {
		urlParams.Set(`max_execution_speed_bytes`, strconv.Itoa(int(params.MaxExecutionSpeedBytes)))
	}

	if params.MaxExecutionSpeed > 0 || params.MaxExecutionSpeedBytes > 0 {
		urlParams.Set(`timeout_before_checking_execution_speed`, `1`)
	}

	if len(params.SessionId) > 0 {
		urlParams.Set(`session_id`, params.SessionId)
		if params.SessionTimeout > 0 {
			urlParams.Set(`session_timeout`, strconv.Itoa(int(params.SessionTimeout)))
		}
	}

	if params.MaxResultRows > 0 {
		urlParams.Set(`max_result_rows`, strconv.Itoa(int(params.MaxResultRows)))
		urlParams.Set(`result_overflow_mode`, `break`)
	}

	if params.MaxExecutionTime > 0 {
		urlParams.Set(`max_execution_time`, strconv.Itoa(int(params.MaxExecutionTime)))
	}

	if params.MaxColumnsToRead > 0 {
		urlParams.Set(`max_columns_to_read`, strconv.Itoa(int(params.MaxColumnsToRead)))
	}

	return urlParams.Encode()
}

func queryServer(deadline time.Time, dst destination.ServerHostPort, query string, params *QueryParams) (httpCode int, res []byte, err error) {
	maxExecSec := deadline.Sub(time.Now())/time.Second + 1

	asyncResult := false
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer func() {
		if !asyncResult {
			cancel()
		}
	}()

	reqUrl := fmt.Sprintf("http://%s/?%s&query=%s", dst, buildUrlParams(maxExecSec, params), url.QueryEscape(query))
	req, err := http.NewRequest("GET", reqUrl, nil)
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
