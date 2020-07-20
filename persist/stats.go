package persist

import (
	"fmt"
	"io/ioutil"
	"strings"
)

// AddStats fills stats keys that correspond to persistent storage
func AddStats(m map[string]string) {
	totalMap := make(map[string]int64)
	pendingMap := make(map[string]int64)

	fis, err := ioutil.ReadDir(conf.Dir)
	if err != nil {
		m["PERSIST_error"] = err.Error()
		return
	}

	for _, fi := range fis {
		relFilename := fi.Name()

		if fi.IsDir() || !strings.HasSuffix(relFilename, logSuffix) {
			continue
		}

		table := getTableFromFileName(relFilename)
		off := getAcknowledgedOffset(relFilename)
		sz := fi.Size()

		totalMap[table] += sz
		if sz > off.Offset {
			pendingMap[table] += sz - off.Offset
		}
	}

	for table, num := range totalMap {
		m["PERSIST_table_size__"+table] = fmt.Sprint(num)
	}

	var totalPending int64

	for table, num := range pendingMap {
		totalPending += num
		m["PERSIST_table_pending__"+table] = fmt.Sprint(num)
	}

	m["PERSIST_total_pending"] = fmt.Sprint(totalPending)
}
