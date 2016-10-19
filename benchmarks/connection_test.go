package benchmarks

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"os"
	"testing"
)

func BenchmarkConnectionCreation(b *testing.B) {
	connectionStruct := common.ConnectionStruct{}

	buffer := make([]byte, 20)
	binary.LittleEndian.PutUint64(buffer[0:8], 1)
	binary.LittleEndian.PutUint64(buffer[8:16], 1)
	binary.LittleEndian.PutUint32(buffer[16:20], 1)

	reader := bytes.NewReader(buffer)
	connectionStruct.Reader = bufio.NewReaderSize(reader, config.Base.NetworkBuffer)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader.Seek(0, os.SEEK_SET)
		connectionStruct.DeserializeHeader()
	}
}
