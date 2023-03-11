package storage

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
	"time"

	"mousedb/pkg/assert"
)

func TestEncodeDecodeEntry(t *testing.T) {
	tStamp := uint32(time.Now().Unix())
	key := []byte("Foo")
	value := []byte("Bar")
	ksz := uint32(len(key))
	valuesz := uint32(len(value))
	buf := make([]byte, HeaderSize+ksz+valuesz)
	binary.LittleEndian.PutUint32(buf[4:8], tStamp)
	binary.LittleEndian.PutUint32(buf[8:12], ksz)
	binary.LittleEndian.PutUint32(buf[12:16], valuesz)
	copy(buf[16:(16+ksz)], key)
	copy(buf[(16+ksz):(16+ksz+valuesz)], value)
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], uint32(c32))

	// Test decode
	ksz = binary.LittleEndian.Uint32(buf[8:12])
	valuesz = binary.LittleEndian.Uint32(buf[12:16])
	tStamp = binary.LittleEndian.Uint32(buf[4:8])
	c32 = binary.LittleEndian.Uint32(buf[:4])
	assert.Equal(t, binary.LittleEndian.Uint32(buf[0:4]), c32)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[4:8]), tStamp)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[8:12]), ksz)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[12:16]), valuesz)
	assert.Equal(t, buf[HeaderSize:(HeaderSize+ksz)], key)
	assert.Equal(t, buf[(HeaderSize+ksz):(HeaderSize+ksz+valuesz)], value)

	// EncodeEntry , ksz = 0, valueSz = 0
	ksz = uint32(0)
	valuesz = uint32(0)
	buf = make([]byte, HeaderSize+ksz+valuesz, HeaderSize+ksz+valuesz)
	binary.LittleEndian.PutUint32(buf[4:8], tStamp)
	binary.LittleEndian.PutUint32(buf[8:12], ksz)
	binary.LittleEndian.PutUint32(buf[12:16], valuesz)
	c32 = crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)
	// decodeEntry, ksz =0, valueSz = 0
	assert.Equal(t, binary.LittleEndian.Uint32(buf[0:4]), c32)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[4:8]), tStamp)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[8:12]), ksz)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[12:16]), valuesz)
}

func TestEncodeDecodeIdx(t *testing.T) {
	tStamp := uint32(time.Now().Unix())
	key := []byte("Foo")
	value := []byte("Bar")
	ksz := uint32(len(key))
	valuesz := uint32(len(value))
	valuePos := uint64(8)
	buf := make([]byte, IdxHeaderSize+ksz, IdxHeaderSize+ksz)
	binary.LittleEndian.PutUint32(buf[0:4], tStamp)
	binary.LittleEndian.PutUint32(buf[4:8], ksz)
	binary.LittleEndian.PutUint32(buf[8:12], valuesz)
	binary.LittleEndian.PutUint64(buf[12:20], valuePos)
	copy(buf[IdxHeaderSize:], key)
	// decodeIdx
	assert.Equal(t, binary.LittleEndian.Uint32(buf[:4]), tStamp)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[4:8]), ksz)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[8:12]), valuesz)
	assert.Equal(t, binary.LittleEndian.Uint64(buf[12:20]), valuePos)
	assert.Equal(t, buf[IdxHeaderSize:], key)

	ksz = 0
	valuesz = 0
	valuePos = 0
	buf = make([]byte, IdxHeaderSize+ksz, IdxHeaderSize+ksz)
	binary.LittleEndian.PutUint32(buf[0:4], tStamp)
	binary.LittleEndian.PutUint32(buf[4:8], ksz)
	binary.LittleEndian.PutUint32(buf[8:12], valuesz)
	binary.LittleEndian.PutUint64(buf[12:20], valuePos)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[:4]), tStamp)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[4:8]), ksz)
	assert.Equal(t, binary.LittleEndian.Uint32(buf[8:12]), valuesz)
	assert.Equal(t, binary.LittleEndian.Uint64(buf[12:20]), valuePos)
}
