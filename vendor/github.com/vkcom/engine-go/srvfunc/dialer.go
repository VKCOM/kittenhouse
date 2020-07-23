package srvfunc

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log"
	"net"
	"os"
	"sync"
)

var (
	emptyDialer = &net.Dialer{}

	hostsCache = struct {
		m map[string]string // hostname => ip
		sync.RWMutex
	}{
		m: make(map[string]string),
	}
)

func skipLongLine(rd *bufio.Reader) (eof bool) {
	for {
		_, isPrefix, err := rd.ReadLine()
		if err == io.EOF {
			return true
		} else if err != nil {
			log.Printf("Error while parsing /etc/hosts: %s", err.Error())
			return true
		}

		if !isPrefix {
			break
		}
	}

	return false
}

func etcHostsLookup(hostname string) string {
	hostsCache.RLock()
	ip, ok := hostsCache.m[hostname]
	hostsCache.RUnlock()
	if ok {
		return ip
	}

	hostsCache.Lock()
	ip, ok = hostsCache.m[hostname]
	if ok {
		hostsCache.Unlock()
		return ip
	}
	defer hostsCache.Unlock()

	fp, err := os.Open("/etc/hosts")
	if err != nil {
		log.Printf("Could not open /etc/hosts: %s", err.Error())
		return ""
	}

	defer fp.Close()

	hostnameBytes := []byte(hostname)

	rd := bufio.NewReader(fp)

	for {
		ln, isPrefix, err := rd.ReadLine()

		if err == io.EOF {
			break
		} else if err != nil {
			log.Printf("Error while reading /etc/hosts: %s", err.Error())
			return ""
		}

		// do not attempt to parse long lines, we should not really have them in /etc/hosts
		if isPrefix {
			if eof := skipLongLine(rd); eof {
				return ""
			}
		}

		if len(ln) <= 1 || ln[0] == '#' {
			continue
		}

		if !bytes.Contains(ln, hostnameBytes) {
			continue
		}

		// 127.0.0.1       localhost loclahost loclhsot lolcahost
		fields := bytes.Fields(ln)

		if len(fields) <= 1 {
			continue
		}

		// ensure that it is IPv4 address because we do not support dual stack in this resolver anyway
		if ip := net.ParseIP(string(fields[0])); ip == nil || ip.To4() == nil {
			continue
		}

		for _, f := range fields[1:] {
			if bytes.Equal(hostnameBytes, f) {
				return string(fields[0])
			}
		}
	}

	return ""
}

// CachingDialer should be used as DialContext function in http.Transport to speed up DNS resolution dramatically.
func CachingDialer(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" && network != "udp" {
		return emptyDialer.DialContext(ctx, network, addr)
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	// check if it already is an IP address, no need to resolve in this case
	if ip := net.ParseIP(host); ip != nil {
		return emptyDialer.DialContext(ctx, network, addr)
	}

	if hostIP := etcHostsLookup(host); hostIP != "" {
		return emptyDialer.DialContext(ctx, network, hostIP+":"+port)
	}

	return emptyDialer.DialContext(ctx, network, addr)
}
