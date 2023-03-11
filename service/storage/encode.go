package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// ErrCrc32 is returned when the CRC32 checksum fails.
var ErrCrc32 = errors.New("checksumIEEE error")

// encodeEntry encodes a timestamp, key size, value size, key and value into a byte slice.
func encodeEntry(timestamp, keySize, valueSize uint32, key, value []byte) []byte {
	bufSize := HeaderSize + keySize + valueSize
	buf := make([]byte, bufSize)
	binary.LittleEndian.PutUint32(buf[4:8], timestamp)
	binary.LittleEndian.PutUint32(buf[8:12], keySize)
	binary.LittleEndian.PutUint32(buf[12:16], valueSize)
	copy(buf[HeaderSize:(HeaderSize+keySize)], key)
	copy(buf[(HeaderSize+keySize):(HeaderSize+keySize+valueSize)], value)

	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)
	return buf
}

// DecodeEntry decodes a byte slice into a value.
func DecodeEntry(buf []byte) ([]byte, error) {
	c32 := binary.LittleEndian.Uint32(buf[:4])
	if crc32.ChecksumIEEE(buf[4:]) != c32 {
		return nil, ErrCrc32
	}
	ksz := binary.LittleEndian.Uint32(buf[8:12])
	valuesz := binary.LittleEndian.Uint32(buf[12:16])
	value := make([]byte, valuesz)
	copy(value, buf[(HeaderSize+ksz):(HeaderSize+ksz+valuesz)])
	return value, nil
}

// DecodeEntryHeader decodes a byte slice into a header.
func DecodeEntryHeader(buf []byte) (uint32, uint32, uint32, uint32) {
	c32 := binary.LittleEndian.Uint32(buf[:4])
	tStamp := binary.LittleEndian.Uint32(buf[4:8])
	ksz := binary.LittleEndian.Uint32(buf[8:12])
	valuesz := binary.LittleEndian.Uint32(buf[12:16])
	return c32, tStamp, ksz, valuesz
}

// decodeEntryDetail decodes a byte slice into a detailed entry.
func decodeEntryDetail(buf []byte) (uint32, uint32, uint32, uint32, []byte, []byte, error) {
	c32 := binary.LittleEndian.Uint32(buf[:4])
	if crc32.ChecksumIEEE(buf[4:]) != c32 {
		return c32, 0, 0, 0, nil, nil, ErrCrc32
	}
	tStamp := binary.LittleEndian.Uint32(buf[4:8])
	ksz := binary.LittleEndian.Uint32(buf[8:12])
	valuesz := binary.LittleEndian.Uint32(buf[12:16])
	if ksz+valuesz == 0 {
		return c32, tStamp, ksz, valuesz, nil, nil, nil
	}
	key := make([]byte, ksz)
	value := make([]byte, valuesz)
	copy(key, buf[HeaderSize:HeaderSize+ksz])
	copy(value, buf[(HeaderSize+ksz):(HeaderSize+ksz+valuesz)])
	return c32, tStamp, ksz, valuesz, key, value, nil
}

// EncodeIdx encodes a idx record.
func EncodeIdx(tStamp, ksz, valueSz uint32, valuePos uint64, key []byte) []byte {
	buf := make([]byte, IdxHeaderSize+len(key))
	binary.LittleEndian.PutUint32(buf[0:4], tStamp)
	binary.LittleEndian.PutUint32(buf[4:8], ksz)
	binary.LittleEndian.PutUint32(buf[8:12], valueSz)
	binary.LittleEndian.PutUint64(buf[12:IdxHeaderSize], valuePos)
	copy(buf[IdxHeaderSize:], key)
	return buf
}

// DecodeIdx decodes a idx record.
func DecodeIdx(buf []byte) (tStamp, ksz, valueSz uint32, valuePos uint64) {
	tStamp = binary.LittleEndian.Uint32(buf[:4])
	ksz = binary.LittleEndian.Uint32(buf[4:8])
	valueSz = binary.LittleEndian.Uint32(buf[8:12])
	valuePos = binary.LittleEndian.Uint64(buf[12:IdxHeaderSize])
	return tStamp, ksz, valueSz, valuePos
}
