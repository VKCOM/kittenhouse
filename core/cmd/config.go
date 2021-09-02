package cmd

import (
	"bufio"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/vkcom/Tirael666/core/destination"
)

/*
Example of config (based on format of db-proxy):

@target_port 3304;
# Forward all unknown tables to Devel ClickHouse
* clk1;
test_table1 clk10;
test_table2 clk20;

# table =srv1*weight =srv2*weight =srv3*weight ...;
cluster_table =clk30*50 =clk40*100;
other_table =clk50*100 =clk60*100 =clk70*100;

*/

type parseConfigState struct {
	defaultPort int

	haveDefault bool
	tables      map[string]struct{}
}

func parseConfigFile(filename string) (result destination.Map, modTs time.Time, hash string, err error) {
	fp, err := os.Open(filename)
	if err != nil {
		return nil, time.Time{}, "", err
	}
	defer fp.Close()

	fi, err := fp.Stat()
	if err != nil {
		return nil, time.Time{}, "", err
	}

	m, configHash, err := parseConfig(fp)
	return m, fi.ModTime(), configHash, err
}

var commentRegex = regexp.MustCompile(`(?m:^\s*#.*$)`)

func parseConfig(fp io.Reader) (result destination.Map, hash string, err error) {
	rd := bufio.NewReader(fp)
	contentsRaw, err := ioutil.ReadAll(rd)
	if err != nil {
		return nil, "", err
	}
	lines := strings.Split(commentRegex.ReplaceAllString(string(contentsRaw), ""), ";")

	res := make(destination.Map)
	state := &parseConfigState{defaultPort: 8123, tables: make(map[string]struct{})}

	for _, ln := range lines {
		ln = strings.TrimSpace(ln)

		if len(ln) == 0 {
			continue
		}

		if err := state.parseLine(ln, res); err != nil {
			return nil, "", err
		}
	}

	if !state.haveDefault {
		return nil, "", errors.New("No default section found")
	}

	for _, settings := range res {
		settings.Init()
	}

	h := md5.New()
	h.Write(contentsRaw)
	return res, fmt.Sprintf("%x", h.Sum(nil)), nil
}

// server adds default port if needed and returns appropriate structure
func (state *parseConfigState) server(host string, weight uint32) destination.Server {
	if !strings.Contains(host, ":") {
		host = fmt.Sprintf("%s:%d", host, state.defaultPort)
	}

	return destination.Server{
		HostPort: destination.ServerHostPort(host),
		Weight:   weight,
	}
}

// parse servers option:
// either of the following:
// host
// =host1*weight1 =host2*weight2 ...
func (state *parseConfigState) servers(hosts []string) (destination.ServersStr, []destination.Server, error) {
	serversStr := destination.ServersStr(strings.Join(hosts, " "))

	if len(hosts) == 0 {
		return "", nil, errors.New("Expected at least 1 server")
	} else if len(hosts) == 1 {
		srv := state.server(hosts[0], 0)
		return serversStr, []destination.Server{srv}, nil
	}

	var servers []destination.Server

	for _, hostDescr := range hosts {
		host, weight, err := parseHostWithWeight(hostDescr)
		if err != nil {
			return "", nil, fmt.Errorf("Could not parse '%s': %s", hostDescr, err.Error())
		}

		servers = append(servers, state.server(host, weight))
	}

	return serversStr, servers, nil
}

// parse '=host1*weight1'
func parseHostWithWeight(arg string) (host string, weight uint32, err error) {
	if len(arg) < 2 {
		return "", 0, errors.New("Host too short")
	}

	if arg[0] != '=' {
		return "", 0, errors.New("Expected '=' as first character")
	}

	arg = arg[1:]
	starPos := strings.IndexByte(arg, '*')
	if starPos <= 0 || starPos >= len(arg)-1 {
		return "", 0, errors.New("Expected format '=host*weight'")
	}

	weight64, err := strconv.ParseUint(arg[starPos+1:], 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("Could not parse weight: %s", err.Error())
	}

	return arg[0:starPos], uint32(weight64), nil
}

// support the following:
// # comments
// @target_port 8123;
// * default_host;
// table host;
// table host:port;
func (state *parseConfigState) parseLine(ln string, res destination.Map) (err error) {
	if len(ln) <= 1 || ln[0] == '#' {
		return nil
	}

	cols := strings.Fields(strings.TrimRight(ln, ";"))

	if len(cols) < 2 {
		return fmt.Errorf("Expected at least 2 columns (got %d) for line '%s'", len(cols), ln)
	}

	action := cols[0]
	options := cols[1:]

	switch action[0] {
	case '*':
		if state.haveDefault {
			return fmt.Errorf("Duplicate default server for line '%s'", ln)
		}
		state.haveDefault = true

		dst, servers, err := state.servers(options)
		if err != nil {
			return fmt.Errorf("Could not parse line '%s': %s", ln, err.Error())
		}

		settings := destination.NewSetting()
		settings.Default = true
		settings.Servers = servers

		res[dst] = settings
	case '@':
		switch act := strings.TrimPrefix(action, "@"); act {
		case "target_port":
			if len(cols) != 2 {
				return fmt.Errorf("Expected exactly 2 columns (got %d) for line '%s'", len(cols), ln)
			}

			state.defaultPort, err = strconv.Atoi(options[0])
			if err != nil {
				return err
			}

			if p := state.defaultPort; p <= 0 || p >= 65536 {
				return fmt.Errorf("Port must be in range [1, 65535] for line '%s'", ln)
			}
		default:
			return fmt.Errorf("Unsupported option '%s' for line '%s'", act, ln)
		}
	default:
		dst, servers, err := state.servers(options)
		if err != nil {
			return fmt.Errorf("Could not parse line '%s': %s", ln, err.Error())
		}

		table := action
		if _, ok := state.tables[table]; ok {
			return fmt.Errorf("Duplicate table '%s' for line '%s'", table, ln)
		}
		state.tables[table] = struct{}{}

		settings, ok := res[dst]
		if !ok {
			settings = destination.NewSetting()
			res[dst] = settings
		} else if settings.Default {
			return fmt.Errorf("Default host '%s' cannot have any custom tables for line '%s'", dst, ln)
		}
		settings.Tables = append(settings.Tables, table)
		settings.Servers = servers
		res[dst] = settings
	}

	return nil
}
