package srvfunc

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	// CPUInfo считает статистику по использованию CPU глобально по ОС
	CPUInfo struct {
		procUsage   map[string]float32
		procUsageMu sync.Mutex

		clockTicks int // Clock ticks/second

		// нагрузка по CPU: средняя за время время (avg) и средняя за последнее время (cur)
		avgCpuUsagePerc  int
		curCpuUsagePerc  int
		cpuUsagesPerc    [60]int
		lastCpuUsagesIdx int

		cpuNum int // кешируем ответ GetCPUNum()
	}
)

// MakeCPUInfo инициализирует сбор статистики
func MakeCPUInfo() *CPUInfo {
	ci := &CPUInfo{}
	ci.init()

	return ci
}

// GetThisProcUsage возвращает текущую статистику использования CPU в целом по системе: us, ni, sy, id, io
// Если от инициализации (MakeCPUInfo) прошло менее секунды, то вернет пустой словарь
func (ci *CPUInfo) GetThisProcUsage() map[string]float32 {
	m := make(map[string]float32)

	ci.procUsageMu.Lock()
	for k, v := range ci.procUsage {
		m[k] = v
	}
	ci.procUsageMu.Unlock()

	return m
}

// GetSelfCpuUsage возвращает статистику по использованию CPU текущим процессом:
//   среднее и последнее использование в % (100 - полностью занято 1 ядро, 800 - полностью заняты 8 ядер и т.п.)
func (ci *CPUInfo) GetSelfCpuUsage() (avgPerc int, curPerc int) {
	return ci.avgCpuUsagePerc, ci.curCpuUsagePerc
}

// GetCPUNum возвращает число ядер (виртуальных) CPU
func (ci *CPUInfo) GetCPUNum() (int, error) {
	if ci.cpuNum == 0 {
		if buf, err := exec.Command(`nproc`).Output(); err != nil {
			return 0, err
		} else if n, err := strconv.Atoi(strings.TrimSpace(string(buf))); err != nil {
			return 0, err
		} else {
			ci.cpuNum = n
		}
	}

	return ci.cpuNum, nil
}

func (ci *CPUInfo) init() {
	ci.procUsage = make(map[string]float32)

	ci.clockTicks = 0
	if out, err := exec.Command(`getconf`, `CLK_TCK`).Output(); err != nil {
	} else if n, err := strconv.ParseUint(string(bytes.TrimSpace(out)), 10, 32); err != nil {
	} else if n > 0 {
		ci.clockTicks = int(n)
	}
	if ci.clockTicks == 0 {
		ci.clockTicks = 100 // стандартный вариант
	}

	go ci.allSystemCpuUsageLoop()
	go ci.thisProcessCpuUsageLoop()
}

func (ci *CPUInfo) allSystemCpuUsageLoop() {
	bytesNl := []byte("\n")
	bytesSpace := []byte(` `)

	titles := []string{`us`, `ni`, `sy`, `id`, `io`}
	titlesPref := 2 // сколько ведущих колонок из выдачи /proc/stat пропускаем

	cntsCur := make([]uint64, len(titles))
	cntsPrev := make([]uint64, len(titles))
	var prevTotal uint64

	tick := time.Tick(1 * time.Second)

	for {
		if buf, err := ioutil.ReadFile(`/proc/stat`); err != nil {
		} else if lines := bytes.SplitN(buf, bytesNl, 2); len(lines) < 2 {
		} else if cols := bytes.Split(lines[0], bytesSpace); len(cols) < (len(titles) + titlesPref) {
		} else if string(cols[0]) != `cpu` {
		} else {
			cols = cols[titlesPref:]

			total := uint64(0)
			for i := len(titles) - 1; i >= 0; i-- {
				if n, err := strconv.Atoi(string(cols[i])); err == nil {
					cntsCur[i] = uint64(n)
					total += uint64(n)
				}
			}

			if prevTotal > 0 {
				ci.procUsageMu.Lock()
				for k := range ci.procUsage {
					ci.procUsage[k] = 0
				}

				diffTotal := total - prevTotal
				for i := range cntsCur {
					if diff := cntsCur[i] - cntsPrev[i]; diff > 0 {
						ci.procUsage[titles[i]] = 100.0 * float32(diff) / float32(diffTotal)
					}
				}
				ci.procUsageMu.Unlock()
			}

			prevTotal = total
			for i, n := range cntsCur {
				cntsPrev[i] = n
			}
		}

		<-tick
	}
}

func (ci *CPUInfo) thisProcessCpuUsageLoop() {
	readProcSelfStat := func() (totalTime float64, startTime float64, err error) {
		raw, err := ioutil.ReadFile(`/proc/self/stat`)
		if err != nil {
			return 0, 0, err
		}

		chunks := bytes.Split(raw, []byte(` `))
		if len(chunks) < 22 {
			return 0, 0, ErrSyscallFail
		}

		var utime, stime, cutime, cstime, starttime int

		if utime, err = strconv.Atoi(string(chunks[13])); err != nil {
			return 0, 0, err
		} else if stime, err = strconv.Atoi(string(chunks[14])); err != nil {
			return 0, 0, err
		} else if cutime, err = strconv.Atoi(string(chunks[15])); err != nil {
			return 0, 0, err
		} else if cstime, err = strconv.Atoi(string(chunks[16])); err != nil {
			return 0, 0, err
		} else if starttime, err = strconv.Atoi(string(chunks[21])); err != nil {
			return 0, 0, err
		}

		totalTime = float64(utime+stime+cutime+cstime) / float64(ci.clockTicks)
		startTime = float64(starttime) / float64(ci.clockTicks)
		return totalTime, startTime, nil
	}

	var (
		startTime     float64
		prevTotalTime float64
		uptime        uint64
		uptimeTs      int64
	)

	ci.lastCpuUsagesIdx = -1

	for {
		if uptime == 0 {
			// из строки вида "350735.47 234388.90" вычитываю только первое значение до точки
			raw, err := ioutil.ReadFile(`/proc/uptime`)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			} else if n, err := fmt.Sscanf(string(raw), `%d`, &uptime); (err != nil) || (n == 0) {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			uptimeTs = time.Now().Unix()
		}

		totalTime, _startTime, err := readProcSelfStat()
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		startTime = _startTime

		if prevTotalTime > 0 {
			currentUptime := float64(uptime) + float64(time.Now().Unix()-uptimeTs)
			ci.avgCpuUsagePerc = int(100 * (totalTime / (currentUptime - startTime)))

			// расчет средней текущей нагрузки через циклический буфер точечных замеров
			ci.lastCpuUsagesIdx++
			ci.lastCpuUsagesIdx = ci.lastCpuUsagesIdx % len(ci.cpuUsagesPerc)
			ci.cpuUsagesPerc[ci.lastCpuUsagesIdx] = int(100 * (totalTime - prevTotalTime))

			avg := 0
			for _, v := range ci.cpuUsagesPerc {
				avg += v
			}
			ci.curCpuUsagePerc = avg / len(ci.cpuUsagesPerc)
		}
		prevTotalTime = totalTime

		time.Sleep(1 * time.Second)
	}
}
