package srvfunc

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

const (
	pagesize = 4096 // замена C.sysconf(C._SC_PAGESIZE)
)

type (
	// MemStats содержит статистику по использованию памяти в байтах
	// @see man proc по /proc/*/statm
	MemStats struct {
		Size  uint64
		Res   uint64
		Share uint64
		Text  uint64
		Lib   uint64
		Data  uint64
		Dt    uint64
	}

	// GCStats содержит статистику по работе GC
	GCStats struct {
		// PauseTotalMs это общее время работы GC в миллисекундах
		PauseTotalMs uint64
		// PauseTotalMcs это общее время работы GC в микросекундах
		PauseTotalMcs uint64
		// LastPausesMs это длительность всех пауз GC в мс с прошлого вызова GetGCStats (но не более размера циклического буфера)
		LastPausesMs []uint64
		// LastPausesMcs это длительность всех пауз GC в микросекундах с прошлого вызова GetGCStats (но не более размера циклического буфера)
		LastPausesMcs []uint64
		// GCCPUFraction это процент времени (real time), потраченного на GC
		GCCPUFraction float64

		prevNumGC uint64
	}
)

// SetMaxRLimitNoFile пробует выставить текущие nofile лимиты (ulimit -n) в максимально разрешенные
// Вернет в случае успеха кортеж (cur, max) значений лимита
func SetMaxRLimitNoFile() ([]uint64, error) {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		return nil, err
	}

	if rLimit.Cur < rLimit.Max {
		rLimit.Cur = rLimit.Max
		syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	}

	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		return nil, err
	}

	return []uint64{uint64(rLimit.Cur), uint64(rLimit.Max)}, nil
}

// GetNumOpenedFile вычисляет количество используемых файловых дескрипторов приложением
// @see man proc по /proc/*/fd/
func GetNumOpenedFile(pid int) (int, error) {
	var path string
	if pid < 1 {
		path = `/proc/self/fd`
	} else {
		path = fmt.Sprintf(`/proc/%d/fd`, pid)
	}

	list, err := ioutil.ReadDir(path)
	if err != nil {
		return 0, err
	}

	return len(list), nil
}

// MakeSigChan создает канал для получения сигналов указанных типов
func MakeSigChan(sig ...os.Signal) chan os.Signal {
	ch := make(chan os.Signal, 10)
	signal.Notify(ch, sig...)
	return ch
}

// GetMemStat возвращает статистику по использованию памяти
// @see man proc по /proc/*/statm
func GetMemStat(pid uint16) (*MemStats, error) {
	var fname string
	if pid > 0 {
		fname = fmt.Sprintf(`/proc/%d/statm`, pid)
	} else {
		fname = `/proc/self/statm`
	}

	fd, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	var m MemStats

	if _, err := fmt.Fscanf(fd, `%d %d %d %d %d %d %d`, &m.Size, &m.Res, &m.Share, &m.Text, &m.Lib, &m.Data, &m.Dt); err != nil {
		return nil, err
	}

	m.Size *= pagesize
	m.Res *= pagesize
	m.Share *= pagesize
	m.Text *= pagesize
	m.Lib *= pagesize
	m.Data *= pagesize
	m.Dt *= pagesize

	return &m, nil
}

// GetGCStats возвращает статистику по работе GC
func GetGCStats() (stat GCStats) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	const NsecInMsec = uint64(time.Millisecond / time.Nanosecond)
	const NsecInMcsec = uint64(time.Microsecond / time.Nanosecond)

	stat.PauseTotalMs = uint64(memStats.PauseTotalNs / NsecInMsec)
	stat.PauseTotalMcs = uint64(memStats.PauseTotalNs / NsecInMcsec)

	stat.GCCPUFraction = 100 * memStats.GCCPUFraction

	mod := uint64(len(memStats.PauseNs))
	numGC := uint64(memStats.NumGC) % mod

	for {
		pauseMs := memStats.PauseNs[stat.prevNumGC] / NsecInMsec
		pauseMcs := memStats.PauseNs[stat.prevNumGC] / NsecInMcsec
		stat.LastPausesMs = append(stat.LastPausesMs, pauseMs)
		stat.LastPausesMcs = append(stat.LastPausesMcs, pauseMcs)

		if stat.prevNumGC = (stat.prevNumGC + 1) % mod; stat.prevNumGC == numGC {
			break
		}
	}

	return
}

// LogRotate переоткрывает указанный файл и подменяем stdout/stderr вывод на этот файл
func LogRotate(prevLogFd *os.File, fname string) (newLogFd *os.File, err error) {
	if prevLogFd != nil {
		prevLogFd.Close()
		prevLogFd = nil
	}

	flag := os.O_CREATE | os.O_APPEND | os.O_WRONLY
	newLogFd, err = os.OpenFile(fname, flag, os.FileMode(0644))
	if err != nil {
		return nil, err
	}

	syscall.Dup2(int(newLogFd.Fd()), syscall.Stdout)
	syscall.Dup2(int(newLogFd.Fd()), syscall.Stderr)

	return newLogFd, nil
}

func SplitNetworkAddr(addr string) (string, uint16, error) {
	if host, portStr, err := net.SplitHostPort(addr); err != nil {
		return host, 0, err
	} else if port, err := net.LookupPort(``, portStr); err != nil {
		return host, 0, err
	} else {
		return host, uint16(port), nil
	}
}
