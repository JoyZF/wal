package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type ChunkType = byte
type SegmentID uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

const (
	// 7 Bytes
	// Checksum Length max 32767 Type
	//    4      2     1
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32 * KB

	fileModePerm = 0644

	// uin32 + uint32 + int64 + uin32
	// segmentId + BlockNumber + ChunkOffset + ChunkSize
	maxLen = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

// Segment represents a single segment file in WAL.
// The segment file is append-only, and the data is written in blocks.
// Each block is 32KB, and the data is written in chunks.
type segment struct {
	id                 SegmentID
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
	cache              *lru.Cache[uint64, []byte]
	header             []byte // chunkHeaderSize
	blockPool          sync.Pool
}

// segmentReader is used to iterate all the data from the segment file.
// You can call Next to get the next chunk data,
// and io.EOF will be returned when there is no data.
type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
}

// block and chunk header, saved in pool.
type blockAndHeader struct {
	block  []byte
	header []byte
}

// ChunkPosition represents the position of a chunk in a segment file.
// Used to read the data from the segment file.
type ChunkPosition struct {
	SegmentId SegmentID
	// BlockNumber The block number of the chunk in the segment file.
	BlockNumber uint32
	// ChunkOffset The start offset of the chunk in the segment file.
	ChunkOffset int64
	// ChunkSize How many bytes the chunk data takes up in the segment file.
	ChunkSize uint32
}

// NewReader creates a new segment reader.
// You can call Next to get the next chunk data,
// and io.EOF will be returned when there is no data.
func (s *segment) NewReader() *segmentReader {
	return &segmentReader{
		segment:     s,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

// Sync flushes the segment file to disk.
func (s *segment) Sync() error {
	if s.closed {
		return nil
	}
	return s.fd.Sync()
}

// Remove removes the segment file.
func (s *segment) Remove() error {
	if !s.closed {
		s.closed = true
		_ = s.fd.Close()
	}

	return os.Remove(s.fd.Name())
}

// Close closes the segment file.
func (s *segment) Close() error {
	if s.closed {
		return nil
	}

	s.closed = true
	return s.fd.Close()
}

// Size returns the size of the segment file.
func (s *segment) Size() int64 {
	size := int64(s.currentBlockNumber) * int64(blockSize)
	return size + int64(s.currentBlockSize)
}

// writeToBuffer calculate chunkPosition for data, write data to bytebufferpool, update segment status
// The data will be written in chunks, and the chunk has four types:
// ChunkTypeFull, ChunkTypeFirst, ChunkTypeMiddle, ChunkTypeLast.
//
// Each chunk has a header, and the header contains the length, type and checksum.
// And the payload of the chunk is the real data you want to Write.
func (s *segment) writeToBuffer(data []byte, chunkBuffer *bytebufferpool.ByteBuffer) (*ChunkPosition, error) {
	startBufferLen := chunkBuffer.Len()
	padding := uint32(0)

	if s.closed {
		return nil, ErrClosed
	}

	// if the left block size can not hold the chunk header, padding the block
	if s.currentBlockSize+chunkHeaderSize >= blockSize {
		// padding if necessary
		if s.currentBlockSize < blockSize {
			p := make([]byte, blockSize-s.currentBlockSize)
			chunkBuffer.B = append(chunkBuffer.B, p...)
			padding += blockSize - s.currentBlockSize

			// a new block
			s.currentBlockNumber += 1
			s.currentBlockSize = 0
		}
	}

	// return the start position of the chunk, then the user can use it to read the data.
	position := &ChunkPosition{
		SegmentId:   s.id,
		BlockNumber: s.currentBlockNumber,
		ChunkOffset: int64(s.currentBlockSize),
	}
	dataSize := uint32(len(data))
	// The entire chunk can fit into the block.
	if s.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		s.appendChunkBuffer(chunkBuffer, data, ChunkTypeFull)
		position.ChunkSize = dataSize + chunkHeaderSize
	} else {
		// If the size of the data exceeds the size of the block,
		// the data should be written to the block in batches.
		var (
			leftSize             = dataSize
			blockCount    uint32 = 0
			currBlockSize        = s.currentBlockSize
		)

		for leftSize > 0 {
			chunkSize := blockSize - currBlockSize - chunkHeaderSize
			if chunkSize > leftSize {
				chunkSize = leftSize
			}

			var end = dataSize - leftSize + chunkSize
			if end > dataSize {
				end = dataSize
			}

			// append the chunks to the buffer
			var chunkType ChunkType
			switch leftSize {
			case dataSize: // First chunk
				chunkType = ChunkTypeFirst
			case chunkSize: // Last chunk
				chunkType = ChunkTypeLast
			default: // Middle chunk
				chunkType = ChunkTypeMiddle
			}
			s.appendChunkBuffer(chunkBuffer, data[dataSize-leftSize:end], chunkType)

			leftSize -= chunkSize
			blockCount += 1
			currBlockSize = (currBlockSize + chunkSize + chunkHeaderSize) % blockSize
		}
		position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	}

	// the buffer length must be equal to chunkSize+padding length
	endBufferLen := chunkBuffer.Len()
	if position.ChunkSize+padding != uint32(endBufferLen-startBufferLen) {
		panic(fmt.Sprintf("wrong!!! the chunk size %d is not equal to the buffer len %d",
			position.ChunkSize+padding, endBufferLen-startBufferLen))
	}

	// update segment status
	s.currentBlockSize += position.ChunkSize
	if s.currentBlockSize >= blockSize {
		s.currentBlockNumber += s.currentBlockSize / blockSize
		s.currentBlockSize = s.currentBlockSize % blockSize
	}
	return position, nil
}

func (s *segment) writeAll(data [][]byte) ([]*ChunkPosition, error) {
	var (
		cp  []*ChunkPosition
		err error
	)

	if s.closed {
		return nil, ErrClosed
	}

	// if any error occurs, rollback the segment status
	originBlockNumber := s.currentBlockNumber
	originBlockSize := s.currentBlockSize

	// init chunk buffer
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			s.currentBlockNumber = originBlockNumber
			s.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	// write all data to the chunk buffer
	var pos *ChunkPosition
	cp = make([]*ChunkPosition, len(data))
	for i := 0; i < len(cp); i++ {
		pos, err = s.writeToBuffer(data[i], chunkBuffer)
		if err != nil {
			return nil, err
		}
		cp[i] = pos
	}
	// write the chunk buffer to the segment file
	if err = s.writeChunkBuffer(chunkBuffer); err != nil {
		return nil, err
	}
	return cp, nil
}

// Write writes the data to the segment file.
func (s *segment) Write(data []byte) (*ChunkPosition, error) {
	var (
		pos *ChunkPosition
		err error
	)

	if s.closed {
		return nil, ErrClosed
	}

	originBlockNumber := s.currentBlockNumber
	originBlockSize := s.currentBlockSize

	// init chunk buffer
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			s.currentBlockNumber = originBlockNumber
			s.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()

	// write all data to the chunk buffer
	pos, err = s.writeToBuffer(data, chunkBuffer)
	if err != nil {
		return nil, err
	}
	// write the chunk buffer to the segment file
	if err = s.writeChunkBuffer(chunkBuffer); err != nil {
		return nil, err
	}

	return pos, err
}

func (s *segment) appendChunkBuffer(buf *bytebufferpool.ByteBuffer, data []byte, chunkType ChunkType) {
	// Length	2 Bytes	index:4-5
	binary.LittleEndian.PutUint16(s.header[4:6], uint16(len(data)))
	// Type	1 Byte	index:6
	s.header[6] = chunkType
	// Checksum	4 Bytes index:0-3
	sum := crc32.ChecksumIEEE(s.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(s.header[:4], sum)

	// append the header and data to segment chunk buffer
	buf.B = append(buf.B, s.header...)
	buf.B = append(buf.B, data...)
}

func (s *segment) writeChunkBuffer(buf *bytebufferpool.ByteBuffer) error {
	if s.currentBlockSize > blockSize {
		panic("wrong! can not exceed the block size")
	}

	// write the data into underlying file
	if _, err := s.fd.Write(buf.Bytes()); err != nil {
		return err
	}
	return nil
}

// Read reads the data from the segment file by the block number and chunk offset.
func (s *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	value, _, err := s.readInternal(blockNumber, chunkOffset)
	return value, err
}

func (s *segment) readInternal(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if s.closed {
		return nil, nil, ErrClosed
	}

	var (
		result    []byte
		bh        = s.blockPool.Get().(*blockAndHeader)
		segSize   = s.Size()
		nextChunk = &ChunkPosition{SegmentId: s.id}
	)

	defer func() {
		s.blockPool.Put(bh)
	}()

	for {
		size := int64(blockSize)
		offset := int64(blockNumber) * blockSize
		if size+offset > segSize {
			size = segSize - offset
		}

		if chunkOffset >= size {
			return nil, nil, io.EOF
		}

		var ok bool
		var cachedBlock []byte
		// try to read from the cache if it is enabled
		if s.cache != nil {
			cachedBlock, ok = s.cache.Get(s.getCacheKey(blockNumber))
		}
		// cache hit, get block from the cache
		if ok {
			copy(bh.block, cachedBlock)
		} else {
			// cache miss, read block from the segment file
			_, err := s.fd.ReadAt(bh.block[0:size], offset)
			if err != nil {
				return nil, nil, err
			}
			// cache the block, so that the next time it can be read from the cache.
			// if the block size is smaller than blockSize, it means that the block is not full,
			// so we will not cache it.
			if s.cache != nil && size == blockSize && len(cachedBlock) == 0 {
				cacheBlock := make([]byte, blockSize)
				copy(cacheBlock, bh.block)
				s.cache.Add(s.getCacheKey(blockNumber), cacheBlock)
			}
		}

		// header
		copy(bh.header, bh.block[chunkOffset:chunkOffset+chunkHeaderSize])

		// length
		length := binary.LittleEndian.Uint16(bh.header[4:6])

		// copy data
		start := chunkOffset + chunkHeaderSize
		result = append(result, bh.block[start:start+int64(length)]...)

		// check sum
		checksumEnd := chunkOffset + chunkHeaderSize + int64(length)
		checksum := crc32.ChecksumIEEE(bh.block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(bh.header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		// type
		chunkType := bh.header[6]

		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd
			// If this is the last chunk in the block, and the left block
			// space are paddings, the next chunk should be in the next block.
			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber += 1
				nextChunk.ChunkOffset = 0
			}
			break
		}
		blockNumber += 1
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

func (s *segment) getCacheKey(blockNumber uint32) uint64 {
	return uint64(s.id)<<32 | uint64(blockNumber)
}

func (sReader *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	// The segment file is closed
	if sReader.segment.closed {
		return nil, nil, ErrClosed
	}

	// this position describes the current chunk info
	chunkPosition := &ChunkPosition{
		SegmentId:   sReader.segment.id,
		BlockNumber: sReader.blockNumber,
		ChunkOffset: sReader.chunkOffset,
	}

	value, nextChunk, err := sReader.segment.readInternal(
		sReader.blockNumber,
		sReader.chunkOffset,
	)
	if err != nil {
		return nil, nil, err
	}

	// Calculate the chunk size.
	// Remember that the chunk size is just an estimated value,
	// not accurate, so don't use it for any important logic.
	chunkPosition.ChunkSize =
		nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) -
			(sReader.blockNumber*blockSize + uint32(sReader.chunkOffset))

	// update the position
	sReader.blockNumber = nextChunk.BlockNumber
	sReader.chunkOffset = nextChunk.ChunkOffset

	return value, chunkPosition, nil
}

// Encode encodes the chunk position to a byte slice.
// Return the slice with the actual occupied elements.
// You can decode it by calling wal.DecodeChunkPosition().
func (cp *ChunkPosition) Encode() []byte {
	return cp.encode(true)
}

// EncodeFixedSize encodes the chunk position to a byte slice.
// Return a slice of size "maxLen".
// You can decode it by calling wal.DecodeChunkPosition().
func (cp *ChunkPosition) EncodeFixedSize() []byte {
	return cp.encode(false)
}

// encode the chunk position to a byte slice.
func (cp *ChunkPosition) encode(shrink bool) []byte {
	buf := make([]byte, maxLen)

	var index = 0
	// SegmentId
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentId))
	// BlockNumber
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	// ChunkOffset
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	// ChunkSize
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))

	if shrink {
		return buf[:index]
	}
	return buf
}

func openSegmentFile(dirPath, extName string, id SegmentID, cache *lru.Cache[uint64, []byte]) (*segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, extName, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)

	if err != nil {
		return nil, err
	}

	// set the current block number and block size.
	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, extName, err))
	}

	return &segment{
		id:                 id,
		fd:                 fd,
		cache:              cache,
		header:             make([]byte, chunkHeaderSize),
		blockPool:          sync.Pool{New: newBlockAndHeader},
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
	}, nil
}

func newBlockAndHeader() interface{} {
	return &blockAndHeader{
		block:  make([]byte, blockSize),
		header: make([]byte, chunkHeaderSize),
	}
}

// DecodeChunkPosition decodes the chunk position from a byte slice.
// You can encode it by calling wal.ChunkPosition.Encode().
func DecodeChunkPosition(buf []byte) *ChunkPosition {
	if len(buf) == 0 {
		return nil
	}

	var index = 0
	// SegmentId
	segmentId, n := binary.Uvarint(buf[index:])
	index += n
	// BlockNumber
	blockNumber, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkOffset
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	// ChunkSize
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n

	return &ChunkPosition{
		SegmentId:   SegmentID(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
