package destination

import (
	"math"
	"math/rand"
	"testing"
)

func TestDistribution(t *testing.T) {
	const iterationCount = 15000
	const maxDeviaton = 50

	rand.Seed(0)

	settings := NewSetting()
	settings.Servers = []Server{
		{HostPort: "db1", Weight: 100},
		{HostPort: "db2", Weight: 50},
	}
	settings.Init()

	resMap := make(map[ServerHostPort]int)

	for i := 0; i < iterationCount; i++ {
		host, ok := settings.ChooseNextServer()
		if !ok {
			t.Errorf("Failed to get next server on iteration #%d", i)
		}
		resMap[host]++
	}

	gotHits := resMap["db1"]
	expectedHits := iterationCount * 2 / 3
	if math.Abs(float64(gotHits)-float64(expectedHits)) > maxDeviaton {
		t.Errorf("db1 got invalid number of hits: %d, expected %d±%d", gotHits, expectedHits, maxDeviaton)
	}

	gotHits = resMap["db2"]
	expectedHits = iterationCount / 3
	if math.Abs(float64(gotHits)-float64(expectedHits)) > maxDeviaton {
		t.Errorf("db2 got invalid number of hits: %d, expected %d±%d", gotHits, expectedHits, maxDeviaton)
	}
}
