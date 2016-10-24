package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"testing"
	"time"
)

func CheckCombustor(t *testing.T, size int) {
	if len(exhaust.CombustorChannel) != size {
		t.Fatalf("combustor channel size %d (should be %d)", len(exhaust.CombustorChannel), size)
	}
}

func CheckRecordsMap(t *testing.T, size int) {
	if len(exhaust.RecordsMap) != size {
		t.Fatalf("record map size %d (should be %d)", len(exhaust.RecordsMap), size)
	}
}

func CheckConnections(t *testing.T, size int) {
	time.Sleep(1e8)
	if len(exhaust.ConnectionsMap) != size {
		t.Fatalf("%d connections instead of %d", len(exhaust.ConnectionsMap), size)
	}
}

func CheckConnectionChannel(t *testing.T, id uint64, size int) {
	exhaust.ConnectionsMutex.RLock()
	actual := len(exhaust.ConnectionsMap[id].Channel)
	exhaust.ConnectionsMutex.RUnlock()
	if actual != size {
		t.Fatalf("%d record in %d connection channel (should be %d)", actual, id, size)
	}
}
