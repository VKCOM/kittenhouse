## KittenHouse

KittenHouse is designed to be a local proxy between ClickHouse and your application server in case you are unable or unwilling to buffer INSERT data on your application side. This is the case for PHP, for example.

KittenHouse is meant to be installed on each host of the cluster that needs to interact with ClickHouse and features in-memory data buffering, on-disk data buffering with retries support, per-table routing, load-balancing, health checking, etc.

## Installation

You will need to do the following to install kittenhouse on your server (the only platform that we tested is Linux but it should work on other platforms as well):

1. Install go (https://golang.org/)
2. `go get github.com/vkcom/kittenhouse`
3. You're done, your binary is located at `$GOPATH/bin/kittenhouse` (e.g. `~/go/bin/kittenhouse`)

## Command-line options

The simplest way to launch kittenhouse is the following:

```sh
$ kittenhouse -u='' -g=''
```

It will launch kittenhouse with current user and group (empty `-u` and `-g` arguments) and expect a single ClickHouse server to be located at `127.0.0.1:8123`.

Here are the essential command-line options that you will most certainly need to modify are the following:

```sh
-u <user>                change daemon user (default is "kitten")
-g <group>               change daemon group (default is "kitten")
-ch-addr <addr>          ClickHouse server address (if you have only one ClickHouse server)
-l <filename>            log file (default is STDERR)
-h <host>                listen host (default is "0.0.0.0" which may be dangerous!)
-p <port>                listen port (default is 13338)
-dir <dirname>           directory where kittenhouse stores persistent content
-db <database name>      ClickHouse database (default is "default")
-ch-user <username>      Clickhouse username
-ch-password <password>  Clickhouse password
-ch-ssl-cert-path <path> Clickhouse SSL certificate path
```

You can see the full list of options by running `kittenhouse --help`.

## Usage

KittenHouse is relatively simple to use: it only has two commands: select and insert. You do selects by doing HTTP GET queries and inserts by sending HTTP POST.

### Default tables

KittenHouse actually writes it's own logs into ClickHouse as well so you will need to create the following tables on your default ClickHouse cluster:

```sql
CREATE TABLE default.internal_logs (
  date Date DEFAULT toDate(time),
  time DateTime,
  server String,
  port Int32,
  type String,
  table String,
  volume Int64,
  message String,
  content String,
  source String,
  server_time DateTime DEFAULT now()
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/internal_logs', '{replica}')
PARTITION BY tuple()
ORDER BY time
SETTINGS index_granularity = 8192;

CREATE TABLE default.internal_logs_buffer AS default.internal_logs ENGINE = Buffer(default, internal_logs, 2, 10, 10, 10000000, 10000000, 100000000, 100000000);

CREATE TABLE default.daemon_heartbeat (
  date Date DEFAULT toDate(time),
  time DateTime,
  daemon String,
  server String,
  port Int32,
  build_version String,
  build_commit String,
  config_ts DateTime,
  config_hash String,
  memory_used_bytes Int64,
  cpu_user_avg Float32,
  cpu_sys_avg Float32,
  server_time DateTime DEFAULT now()
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/daemon_heartbeat', '{replica}', date, (daemon, server, port), 8192, server_time);

CREATE TABLE default.daemon_heartbeat_buffer AS default.daemon_heartbeat ENGINE = Buffer(default, daemon_heartbeat, 2, 15, 15, 10000000, 10000000, 100000000, 100000000);
```

You can use usual non-replicated `*MergeTree` versions if you do not have replication, although we strongly suggest that you have at least 2 ClickHouse servers in order not to lose data.
If you are using replication, we assume that you have `{shard}` and `{replica}` macros set up properly.

### INSERT

Default format for INSERT queries is VALUES (SQL, text-based). You can pretty much use your ORM or escaping code for MySQL in order to use VALUES, so that is why it is the default.

In order to do INSERT to some table, you send HTTP POST to kittenhouse port:

```sh
HTTP POST /

Query string params:

   table      - table where you are INSERTing, must contain column names (e.g. "test(a,b,c)")
   debug      - set debug=1 to do INSERT synchronously
   persistent - set persistent=1 if you need to write data to disk (default is in-memory)
   rowbinary  - set rowbinary=1 if you send data in RowBinary format instead of VALUES

POST body:

   POST body is the actual VALUES or RowBinary that you are inserting.
   POST body can contain many rows. Max POST body size is 16 MiB.
   If you insert multiple rows in VALUES format, rows must be comma-separated, as in usual INSERT.
```

Example using curl:

```sh
$ curl -d "('2018-11-14 10:00:00','yourserver',123)" -v 'http://localhost:13338/?table=internal_logs_buffer(time,server,port)'
```


### SELECT

Only queries in form `SELECT ... FROM ...` are accepted. You send queries by sending GET query to kittenhouse with the following parameters:

```
HTTP GET /

Query string params:

  query     - SQL query in form 'SELECT ... FROM ...'
  timeout   - timeout, in seconds (default 30)
```

Example using curl (get current hostname from server):

```sh
$ curl -v 'http://127.0.0.1:13338/?query=SELECT%20hostName()%20FROM%20system.one'
```

## Per-table routing and load balancing

If you have more than 1 ClickHouse server (which you probably do) you will need to be able to route INSERTs and SELECTs to different tables into different clusters. Is is supported in kittenhouse using the following (a bit weird) syntax:

```ini
# this is a comment

# override default port with 8125 (default is 8123, ClickHouse HTTP interface)
@target_port 8125;

# set default cluster to be just server "clk1":
* clk1;

# route test_table1 to just server "clk10"
test_table1 clk10;

# routing to serveral servers with weights:
# table =srv1*weight =srv2*weight =srv3*weight ...;

# route cluster_table to server clk30 with weight 50, and to server clk40 with weight 100
cluster_table =clk30*50 =clk40*100;
```

You then provide the config file using `-c <config-file>` option when launching `kittenhouse`.

## Reverse proxy

Kittenhouse is shipped with it's own reverse proxy if you have a high number of servers with kittenhouse (>100) and you do not want to use nginx as a reverse proxy for ClickHouse (it has some drawbacks but they aren't fatal).

You run it the following way:

```sh
$ kittenhouse -p 8124 -reverse -ch-addr=127.0.0.1:8123
```

It will start listening on port `8124` and act as a reverse proxy to ClickHouse server at `http://127.0.0.1:8123/`.

### Reverse proxy Features

1. Custom KITTEN/MEOW protocol to be able to handle tens of thousands active TCP connections without consuming too much memory.
2. Table-aware buffering
3. Separate connection pools for SELECT and for INSERT queries
4. Uses fasthttp so it can handle 100K+ separate INSERTs per second

### Reverse proxy drawbacks

1. You cannot INSERT more than 16 MiB in a single request.
2. POST bodies are fully buffered in memory before being sent to ClickHouse. This can consume a LOT of memory and that is the reason why KITTEN/MEOW protocol is used if available.
3. Results for SELECT queries are fully buffered in memory before the result is sent to client. This can also lead to OOM.
