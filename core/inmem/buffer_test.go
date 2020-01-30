package inmem

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
)

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
