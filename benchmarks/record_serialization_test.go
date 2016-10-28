package benchmarks

import (
	"bufio"
	"github.com/rambler-digital-solutions/thrustmq/common"
	"github.com/rambler-digital-solutions/thrustmq/config"
	"math/rand"
	"os"
	"testing"
)

func getBufferedWriter() *bufio.Writer {
	indexFile, err := os.OpenFile(config.Base.IndexPrefix+"_bench", os.O_RDWR|os.O_CREATE, 0666)
	common.FaceIt(err)
	indexFile.Seek(0, os.SEEK_SET)
	return bufio.NewWriterSize(indexFile, config.Base.FileBuffer)
}

func getBufferedReader() *bufio.Reader {
	indexFile, err := os.OpenFile(config.Base.IndexPrefix+"_bench", os.O_RDONLY|os.O_CREATE, 0666)
	common.FaceIt(err)
	indexFile.Seek(0, os.SEEK_SET)
	return bufio.NewReaderSize(indexFile, config.Base.FileBuffer)
}

func getFile() *os.File {
	indexFile, err := os.OpenFile(config.Base.IndexPrefix+"_bench", os.O_RDONLY|os.O_CREATE, 0666)
	common.FaceIt(err)

	return indexFile
}

func prepare(n int) {
	BenchmarkRecordSerialization(&testing.B{N: n})
}

func BenchmarkRecordSerialization(b *testing.B) {
	indexWriter := getBufferedWriter()
	for i := 0; i < b.N; i++ {
		record := common.Record{}
		slots := record.Slots()
		for i := 0; i < len(slots); i++ {
			*slots[i] = uint64(rand.Int63())
		}
		record.Connection = uint64(i + 1)
		indexWriter.Write(record.Serialize())
	}
	indexWriter.Flush()
}

func BenchmarkRecordSequentialDeserialization(b *testing.B) {
	prepare(b.N)
	b.ResetTimer()

	indexReader := getBufferedReader()
	for i := 0; i < b.N; i++ {
		readRecord := common.Record{}
		readRecord.Deserialize(indexReader)
		if readRecord.Connection != uint64(i+1) {
			b.Errorf("data corruption at record #%d %d", i, readRecord.Connection)
		}
	}
}

func BenchmarkRecordRandomDeserialization(b *testing.B) {
	prepare(b.N)
	b.ResetTimer()

	indexFile := getFile()
	for i := 0; i < b.N; i++ {
		indexFile.Seek(int64(b.N-1-i)*int64(common.IndexSize), os.SEEK_SET)
		readRecord := common.Record{}
		readRecord.Deserialize(indexFile)
		if readRecord.Connection != uint64(b.N-i) {
			b.Errorf("data corruption at record #%d %d", i, readRecord.Connection)
		}
	}
}
