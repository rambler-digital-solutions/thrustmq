package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
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

func CheckChunkNumber(t *testing.T, expectation uint64) {
	actualChunkNumber := common.State.ChunkNumber()
	if actualChunkNumber != expectation {
		t.Fatalf("chunk number mismatch! got: %d expected: %d", actualChunkNumber, expectation)
	}
}

func CheckUncompressedMessages(t *testing.T, expectation int) {
	if len(intake.CompressorStage2Channel) != expectation {
		t.Fatalf("there are uncompressed messages: %d messages instead of %d", len(intake.CompressorStage2Channel), expectation)
	}
}

func CheckConnectionChannel(t *testing.T, ID uint64, size int) {
	connection := exhaust.ConnectionsMapGet(ID)
	if connection == nil {
		t.Fatalf("connection is closed!")
	}
	if len(connection.Channel) != size {
		t.Fatalf("%d record in %d connection channel (should be %d)", len(connection.Channel), ID, size)
	}
}
