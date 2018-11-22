package persist

import (
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
	if res := getTableFromFileName(getTableFileName("video_log_buffer (...)")); res != "video_log_buffer" {
		t.Errorf("Could not parse filename: got '%s' instead of 'video_log_buffer'", res)
	}

	if res := getTableFromFileName(getTableFileName("video_log(...)")); res != "video_log" {
		t.Errorf("Could not parse filename: got '%s' instead of 'video_log'", res)
	}

	if res := getTableFromFileName("random_stuff"); res != "" {
		t.Errorf("Could not parse filename: got '%s' instead of ''", res)
	}
}
