package srvfunc

import (
	"bufio"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

var (
	ErrNotFound    = errors.New(`Not found`)
	ErrSyscallFail = errors.New(`Syscall fail`)
)

func parseEtcPasswdGroup(fname string) (map[string][]string, error) {
	fd, err := os.Open(fname)
	if err != nil {
		return nil, EWrap(err)
	}
	defer fd.Close()

	rd := bufio.NewReader(fd)
	m := make(map[string][]string)

	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, EWrap(err)
		}

		chunks := strings.Split(line, `:`)
		if len(chunks) > 1 {
			m[chunks[0]] = chunks[1:]
		}
	}

	return m, nil
}

// LookupUidByName ищет сведения по пользователю в /etc/passwd
func LookupUidByName(name string) (int, error) {
	if m, err := parseEtcPasswdGroup(`/etc/passwd`); err != nil {
		return -1, err
	} else if user, ok := m[name]; !ok {
		return -1, EWrap(ErrNotFound)
	} else if n, err := strconv.ParseUint(user[1], 10, 32); err != nil {
		return -1, err
	} else {
		return int(n), nil
	}
}

// LookupGidByName ищет сведения по группе с /etc/group
func LookupGidByName(name string) (int, error) {
	if m, err := parseEtcPasswdGroup(`/etc/group`); err != nil {
		return -1, err
	} else if group, ok := m[name]; !ok {
		return -1, EWrap(ErrNotFound)
	} else if n, err := strconv.ParseUint(group[1], 10, 32); err != nil {
		return -1, err
	} else {
		return int(n), nil
	}
}
