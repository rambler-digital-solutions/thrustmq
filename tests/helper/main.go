package helper

import (
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/consumer"
	"github.com/rambler-digital-solutions/thrustmq/clients/golang/producer"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/logging"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/exhaust"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/intake"
	"math/rand"
	"testing"
	"time"
)

var (
	intakeInitialized  = false
	exhaustInitialized = false
)

func BootstrapIntake(t *testing.T) {
	if !intakeInitialized {
		logging.Init()
		go intake.Init()
		rand.Seed(time.Now().UTC().UnixNano())
		time.Sleep(1e6)
		intakeInitialized = true
	}
	producer.Disconnect()
	producer.Connect()
}

func BootstrapExhaust(t *testing.T) {
	if !exhaustInitialized {
		rand.Seed(time.Now().UTC().UnixNano())
		logging.Init()
		common.State.Tail = common.State.Head
		go exhaust.Init()
		time.Sleep(1e7)
		exhaustInitialized = true
	}

	consumer.Disconnect()
	CheckConnections(t, 0)
	consumer.Connect()
	CheckConnections(t, 1)
}

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
	actual := len(exhaust.ConnectionsMap[common.State.ConnectionId].Channel)
	if actual != size {
		t.Fatalf("%d record in %d connection channel (should be %d)", actual, id, size)
	}
}
