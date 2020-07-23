package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/vkcom/kittenhouse/clickhouse"
	"github.com/vkcom/kittenhouse/inmem"
	"github.com/vkcom/kittenhouse/persist"
)

func serveHTTP() {
	h := makeHTTPServer()
	srv := &fasthttp.Server{
		MaxRequestBodySize: 16 << 20,
		Handler:            h.handleRequest,
	}

	go func() {
		if err := srv.ListenAndServe(fmt.Sprintf("%s:%d", argv.host, argv.port)); err != nil {
			log.Fatalf("Could not listen http: %v", err)
		}
	}()
}

type httpServer struct{}

func makeHTTPServer() *httpServer {
	return &httpServer{}
}

var (
	// query (GET)
	queryKeyBytes   = []byte("query")   // SQL query in form 'SELECT ... FROM ...'
	queryKeyTimeout = []byte("timeout") // timeout, in seconds

	// insert (POST)
	queryKeyTable      = []byte("table")      // table name with columns, e.g. table(a,b,c)
	queryKeyDebug      = []byte("debug")      // set debug=1      to do INSERT synchronously
	queryKeyPersistent = []byte("persistent") // set persistent=1 if you need to write data to disk (in-memory otherwise)
	queryKeyRowBinary  = []byte("rowbinary")  // set rowbinary=1  if you send data in RowBinary format instead of VALUES
)

func (srv *httpServer) handleGET(ctx *fasthttp.RequestCtx) {
	var query string
	timeout := 30 * time.Second

	args := ctx.QueryArgs()

	if queryBytes := args.PeekBytes(queryKeyBytes); queryBytes != nil {
		query = string(queryBytes)
	} else {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("GET-parameter `query` is missing")
		return
	}

	if t, err := args.GetUfloat("timeout"); err == nil {
		timeout = time.Duration(t * float64(time.Second))
	}

	var params clickhouse.QueryParams
	httpCode, res, err := clickhouse.QueryDeadline(time.Now().Add(timeout), query, &params)

	if err != nil {
		ctx.SetStatusCode(500)
		ctx.WriteString(err.Error())
	} else {
		ctx.SetStatusCode(httpCode)
		ctx.Write(res)
	}
}

func (srv *httpServer) handlePOST(ctx *fasthttp.RequestCtx) {
	var table string

	args := ctx.QueryArgs()

	if tableBytes := args.PeekBytes(queryKeyTable); tableBytes != nil {
		table = string(tableBytes)
	} else {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		ctx.WriteString("GET-parameter `table` is missing")
		return
	}

	data := ctx.PostBody()
	rowbinary := args.GetUintOrZero("rowbinary") == 1

	if args.GetUintOrZero("debug") == 1 {
		tableWithColumns := string(table)
		tableClean := tableWithColumns
		if idx := strings.IndexByte(tableClean, '('); idx >= 0 {
			tableClean = tableWithColumns[0:idx]
		}
		err := clickhouse.Flush(
			clickhouse.GetDestinationSetting(strings.TrimSpace(tableClean)),
			tableWithColumns,
			[]byte(data),
			rowbinary,
		)
		if err != nil {
			ctx.SetStatusCode(500)
			ctx.WriteString(err.Error())
			return
		}
	} else if args.GetUintOrZero("persistent") == 1 {
		if err := persist.Write(string(table), data, rowbinary); err != nil {
			ctx.SetStatusCode(500)
			ctx.WriteString(err.Error())
			return
		}
	} else {
		if err := inmem.Write(string(table), data, rowbinary); err != nil {
			ctx.SetStatusCode(500)
			ctx.WriteString(err.Error())
			return
		}
	}
}

func (src *httpServer) handleRequest(ctx *fasthttp.RequestCtx) {
	if ctx.IsPost() {
		src.handlePOST(ctx)
		return
	}

	if ctx.IsGet() {
		src.handleGET(ctx)
		return
	}

	ctx.Response.Header.SetStatusCode(fasthttp.StatusMethodNotAllowed)
}
