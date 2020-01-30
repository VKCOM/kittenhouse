package persist

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

func TestAppendCrcHex(t *testing.T) {
	if res := string(appendCrcHex(nil, 0x12345678)); res != "12345678" {
		t.Errorf("Wrong format for 12345678: got %s", res)
	}

	if res := string(appendCrcHex(nil, 0x003A5678)); res != "003A5678" {
		t.Errorf("Wrong format for 003A5678: got %s", res)
	}
}

func TestParseFileName(t *testing.T) {
	if res := getTableFromFileName(getTableFileName("video_log_buffer (...)", false)); res != "video_log_buffer" {
		t.Errorf("Could not parse filename: got '%s' instead of 'video_log_buffer'", res)
	}

	if res := getTableFromFileName(getTableFileName("video_log(...)", false)); res != "video_log" {
		t.Errorf("Could not parse filename: got '%s' instead of 'video_log'", res)
	}

	if res := getTableFromFileName("random_stuff"); res != "" {
		t.Errorf("Could not parse filename: got '%s' instead of ''", res)
	}
}

func TestBuffer(t *testing.T) {
	var wg sync.WaitGroup
	file := "table1"
	const len = 4

	append := func(i int) {
		defer wg.Done()
		k := ("Key:" + strconv.Itoa(i))
		err := Write(fmt.Sprintf("%s.%d", file, i%2), []byte(k), true)
		if err != nil {
			t.Error(err)
		}
	}
	for i := 1; i <= len; i++ {
		wg.Add(1)
		go append(i)

	}
	wg.Wait()
	//fmt.Println(string(tableBufMap.v["table1.0"].values.b.Bytes()))
}
