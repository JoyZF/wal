package benchmark

import (
	"github.com/JoyZF/wal"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strings"
	"testing"
)

var walFile *wal.WAL

func init() {
	var err error
	walFile, err = wal.Open(wal.Options{
		//DirPath:        os.TempDir(),
		DirPath:        "./default/",
		SegmentSize:    wal.GB,
		SegmentFileExt: ".SEG",
		BlockCache:     32 * wal.KB,
		Sync:           false,
		BytesPerSync:   0,
	})
	if err != nil {
		panic(err)
	}
}

func BenchmarkWAL_Write(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
	}
}

func BenchmarkWAL_WriteBatch(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 31; j++ {
			walFile.PendingWrites([]byte(strings.Repeat("X", wal.MB)))
		}
		walFile.PendingWrites([]byte(strings.Repeat("X", wal.MB)))
		pos, err := walFile.WriteAll()
		assert.Nil(b, err)
		assert.Equal(b, 32, len(pos))
	}
}

func BenchmarkWriteLargeSize(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	content := []byte(strings.Repeat("X", 256*wal.KB+500))
	for i := 0; i < b.N; i++ {
		_, err := walFile.Write(content)
		assert.Nil(b, err)
	}
}

func BenchmarkWAL_Read(b *testing.B) {
	var positions []*wal.ChunkPosition
	for i := 0; i < 1000000; i++ {
		pos, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := walFile.Read(positions[rand.Intn(len(positions))])
		assert.Nil(b, err)
	}
}
