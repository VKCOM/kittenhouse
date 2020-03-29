package inmem

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"strings"
	"testing"
)

func TestWriteBuf(t *testing.T) {
	for i := 0; i < 10000; i++ {
		t.Run(fmt.Sprintf("iteration %d", i), testWriteBufIteration)
	}
}

func TestWriteBufSimple(t *testing.T) {
	w := &writeBuf{}
	w.write("table", []byte("a"), false)
	w.write("table", []byte("b"), false)
	w.write("table", []byte("c"), false)
	w.write("table", []byte("d"), false)

	buf, err := ioutil.ReadAll(w)
	if err != nil {
		t.Fatalf("Expected no errors reading from writeBuf, got %v", err)
	}

	want := []byte("abcd")
	if !bytes.Equal(buf, want) {
		t.Fatalf("Results not equal: want '%s', got '%s'", want, buf)
	}
}

func testWriteBufIteration(t *testing.T) {
	t.Helper()

	var b bytes.Buffer
	w := &writeBuf{}

	chunksCount := rand.Intn(10) + 1
	for i := 0; i < chunksCount; i++ {
		randStr := []byte(strings.Repeat(string(rune(i)+'A'), rand.Intn(100)+1))
		b.Write(randStr)
		w.write("table", randStr, false)
	}

	randStr := []byte(strings.Repeat("Z", 1024))
	b.Write(randStr)
	w.write("table", randStr, false)

	chunksCount = rand.Intn(10) + 1
	for i := 0; i < chunksCount; i++ {
		randStr := []byte(strings.Repeat(string(rune(i)+'A'), rand.Intn(100)+1))
		b.Write(randStr)
		w.write("table", randStr, false)
	}

	buf, err := ioutil.ReadAll(w)
	if err != nil {
		t.Fatalf("Expected no errors reading from writeBuf, got %v", err)
	}

	want := b.Bytes()
	if !bytes.Equal(buf, want) {
		t.Fatalf("Results not equal: want '%s', got '%s'", want, buf)
	}
}
