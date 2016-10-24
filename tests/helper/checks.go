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
	if exhaust.RecordsMapLength() != size {
		t.Fatalf("record map size %d (should be %d)", exhaust.RecordsMapLength(), size)
	}
}

func CheckConnections(t *testing.T, size int) {
	time.Sleep(1e8)
	if exhaust.ConnectionsMapLength() != size {
		t.Fatalf("%d connections instead of %d", exhaust.ConnectionsMapLength(), size)
	}
}

func CheckConnectionChannel(t *testing.T, id uint64, size int) {
	connection := exhaust.ConnectionsMapGet(id)
	if connection == nil {
		t.Fatalf("connection is closed!")
	}
	if len(connection.Channel) != size {
		t.Fatalf("%d record in %d connection channel (should be %d)", len(connection.Channel), id, size)
	}
}
