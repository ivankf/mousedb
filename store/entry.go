package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

// CRC | tsamp | ksz | vsz | key | value
// 4 B | 8 B   | 4B  | 4B

type DataEntry struct {
	Checksum  uint32
	Key       []byte // 键
	Value     []byte // 值
	Timestamp int64  // 时间戳
}

// NewEntry creates a new `DataEntry` with the given `key` and `value`
func NewEntry(key, value []byte, time *time.Time) DataEntry {
	checksum := crc32.ChecksumIEEE(value)

	return DataEntry{
		Checksum:  checksum,
		Key:       key,
		Value:     value,
		Timestamp: time.UnixNano(),
	}
}

func DecodeEntry(data []byte) (*DataEntry, error) {
	if len(data) < 20 {
		errors.New("data is to short")
	}
	checkSum := binary.BigEndian.Uint32(data[:4])
	ksz := binary.BigEndian.Uint32(data[12:16])

	entry := &DataEntry{
		Timestamp: int64(binary.BigEndian.Uint64(data[4:12])),
		Key:       data[20 : 20+ksz],
		Value:     data[20+ksz:],
	}
	if crc32.ChecksumIEEE(entry.Value) != checkSum {
		return nil, errors.New("data is incorrect")
	}
	return entry, nil
}

func EncodeEntry(entry *DataEntry) ([]byte, error) {
	ksz := uint32(len(entry.Key))
	vsz := uint32(len(entry.Value))

	data := make([]byte, 20+ksz+vsz)
	binary.BigEndian.PutUint32(data[:4], crc32.ChecksumIEEE(entry.Value))
	binary.BigEndian.PutUint64(data[4:12], uint64(entry.Timestamp))
	binary.BigEndian.PutUint32(data[12:16], ksz)
	binary.BigEndian.PutUint32(data[16:20], vsz)
	copy(data[20:20+ksz], entry.Key)
	copy(data[20+ksz:], entry.Value)

	return data, nil
}
