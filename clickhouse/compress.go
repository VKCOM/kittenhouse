package clickhouse

import (
	"encoding/binary"

	"github.com/pierrec/lz4"
)

const (
	checksumSize              = 16
	compressedBlockHeaderSize = 9

	compressionMethodNone = 0x02
	compressionMethodLz4  = 0x82
	compressionMethodZstd = 0x90
)

// format of clickhouse compression can be seen here:
// https://github.com/yandex/ClickHouse/blob/5b113df16c6b9e6464cb55fb04280a8a5480794a/dbms/src/IO/CompressedReadBufferBase.cpp#L41
func compress(buf []byte) []byte {
	var res = make([]byte, lz4.CompressBlockBound(len(buf))+compressedBlockHeaderSize+checksumSize)
	compressedLen, _ := lz4.CompressBlock(buf, res[checksumSize+compressedBlockHeaderSize:], nil)

	res[checksumSize] = compressionMethodLz4
	binary.LittleEndian.PutUint32(res[checksumSize+1:], compressedBlockHeaderSize+uint32(compressedLen))
	binary.LittleEndian.PutUint32(res[checksumSize+5:], uint32(len(buf)))

	return res[0 : checksumSize+compressedBlockHeaderSize+compressedLen]
}
