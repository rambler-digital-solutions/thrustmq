package exhaust

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"github.com/rambler-digital-solutions/thrustmq/subsystems/oplog"
	"os"
	"runtime"
)

func turbine() {
	indexFile, err := os.OpenFile(config.Base.Index, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	dataFile, err := os.OpenFile(config.Base.Data, os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	defer indexFile.Close()
	defer dataFile.Close()

	for {
		markPass(indexFile)
		if len(ConnectionsMap) > 0 {
			fluxPass(indexFile, dataFile)
		}
		runtime.Gosched()
	}
}

func fillMapa(mapa map[uint64]common.IndexRecord) {
	for {
		select {
		case marker := <-TurbineChannel:
			if _, ok := mapa[marker.Seek]; ok {
				if mapa[marker.Seek].Ack < marker.Ack {
					if marker.Ack > 2 {
						marker.Ack = 0
					}
					mapa[marker.Seek] = marker
				}
			} else {
				if marker.Ack > 2 {
					marker.Ack = 0
				}
				mapa[marker.Seek] = marker
			}
		default:
			return
		}
	}
}

func markPass(file *os.File) {
	mapa := make(map[uint64]common.IndexRecord)

	fillMapa(mapa)

	for _, marker := range mapa {
		_, err := file.Seek(int64(marker.Seek), os.SEEK_SET)
		common.FaceIt(err)

		record := common.IndexRecord{}
		record.Deserialize(file)

		record.Ack = marker.Ack
		record.Connection = marker.Connection

		_, err = file.Seek(int64(marker.Seek), os.SEEK_SET)
		common.FaceIt(err)
		file.Write(record.Serialize())
	}
}

func fluxPass(file *os.File, dataFile *os.File) {
	file.Sync()
	stat, err := file.Stat()
	State.Head = uint64(stat.Size())

	if State.Head == 0 {
		return
	}

	reader := bufio.NewReader(file)
	total := float32(State.Head-State.Tail) / float32(common.IndexSize)
	marked := float32(0)
	streak := true
	record := common.IndexRecord{}

	_, err = file.Seek(int64(State.Tail), os.SEEK_SET)
	common.FaceIt(err)

	for ptr := State.Tail; ptr < State.Head-common.IndexSize; ptr += common.IndexSize {
		if len(TurbineChannel) > config.Exhaust.TurbineBuffer/2 {
			return
		}
		if len(CombustorChannel) > config.Exhaust.CombustionBuffer/2 {
			return
		}

		record.Deserialize(reader)

		if record.Ack != 0 {
			marked++
		} else {
			if record.Connection == 0 {
				return
			}
			if _, ok := ConnectionsMap[record.Connection]; !ok {
				record.Ack = 3
				record.Seek = ptr
				oplog.Requeued++

				select {
				case TurbineChannel <- record:
				default:
					return
				}

				message := common.MessageStruct{}
				message.Load(dataFile, record)
				select {
				case CombustorChannel <- message:
				default:
					return
				}
			} else {
				streak = false
			}
		}
		if streak {
			State.Tail = ptr
		}
	}
	State.Capacity = 1 - marked/total
}
