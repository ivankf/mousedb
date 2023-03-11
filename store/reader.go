package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

type KeyValue struct {
	Key   []byte
	Value []byte
}

type Reader struct {
	file   *os.File
	offset int64
}

func NewReader(filename string) (*Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	// check magic number
	var magic [1]byte
	_, err = io.ReadFull(file, magic[:])
	if err != nil {
		return nil, err
	}
	if magic[0] != Magic {
		return nil, errors.New("invalid magic number")
	}

	return &Reader{
		file:   file,
		offset: HeaderSize,
	}, nil
}

func (r *Reader) Next() (*KeyValue, error) {
	var (
		crc        uint32
		timestamp  int64
		keySize    uint32
		valueSize  uint32
		key, value []byte
	)

	// read crc
	err := binary.Read(r.file, binary.BigEndian, &crc)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// read timestamp
	err = binary.Read(r.file, binary.BigEndian, &timestamp)
	if err != nil {
		return nil, err
	}

	// read key_size
	err = binary.Read(r.file, binary.BigEndian, &keySize)
	if err != nil {
		return nil, err
	}

	// read value_size
	err = binary.Read(r.file, binary.BigEndian, &valueSize)
	if err != nil {
		return nil, err
	}

	// read key
	key = make([]byte, keySize)
	_, err = io.ReadFull(r.file, key)
	if err != nil {
		return nil, err
	}

	// read value
	value = make([]byte, valueSize)
	_, err = io.ReadFull(r.file, value)
	if err != nil {
		return nil, err
	}

	// calculate crc
	data := bytes.Join([][]byte{key, value}, nil)
	if calculatedCrc := crc32.Checksum(data, crc32.MakeTable(crc32.IEEE)); calculatedCrc != crc {
		return nil, errors.New("invalid crc")
	}

	r.offset += HeaderSize + int64(keySize) + int64(valueSize)

	return &KeyValue{
		Key:   key,
		Value: value,
	}, nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}
