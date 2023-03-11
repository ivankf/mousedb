package storage

import (
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	HeaderSize    = 16 // 4 + 4 + 4 + 4: crc32 + timestamp + keySize + valueSize
	IdxHeaderSize = 20 // 4 + 4 + 4 + 8: timestamp + keySize + valueSize + valueOffset
	BSM           = ".bsm"
	IDX           = ".idx"
)

// BFiles represents a collection of BFile objects.
type BFiles struct {
	bfs    map[uint32]*BFile
	rwLock *sync.RWMutex
}

// newBFiles returns a new instance of BFiles.
func newBFiles() *BFiles {
	return &BFiles{
		bfs:    make(map[uint32]*BFile),
		rwLock: &sync.RWMutex{},
	}
}

// get retrieves a BFile object by fileID.
func (bfs *BFiles) get(fileID uint32) *BFile {
	bfs.rwLock.RLock()
	defer bfs.rwLock.RUnlock()
	bf, _ := bfs.bfs[fileID]
	return bf
}

// put adds a new BFile object to the collection.
func (bfs *BFiles) put(bf *BFile, fileID uint32) {
	bfs.rwLock.Lock()
	defer bfs.rwLock.Unlock()
	bfs.bfs[fileID] = bf
}

// close closes all BFile objects in the collection.
func (bfs *BFiles) close() {
	bfs.rwLock.Lock()
	defer bfs.rwLock.Unlock()
	for _, bf := range bfs.bfs {
		bf.fp.Close()
		bf.idxFp.Close()
	}
}

// BFile represents a writable data file and its associated idx file.
type BFile struct {
	fp          *os.File
	fileID      uint32
	writeOffset uint64
	idxFp       *os.File
}

// openBFile opens an existing BFile object by dirName and tStamp.
func newBFile() *BFile {
	return &BFile{}
}

func openBFile(dirName string, tStamp int) (*BFile, error) {
	filename := fmt.Sprintf("%s/%d%s", dirName, tStamp, BSM)
	fp, err := os.OpenFile(filename, os.O_RDONLY|os.O_SYNC, 0600)
	if err != nil {
		return nil, err
	}
	return &BFile{
		fileID:      uint32(tStamp),
		fp:          fp,
		idxFp:       nil,
		writeOffset: 0,
	}, nil
}

// read reads the value associated with a given offset and length.
func (bf *BFile) read(offset uint64, length uint32) ([]byte, error) {
	value := make([]byte, length)
	//TODO
	// assert read function and crc32
	if _, err := bf.fp.ReadAt(value, int64(offset)); err != nil {
		return nil, err
	}
	return value, nil
}

// writeData writes a key-value pair to the BFile object.
func (bf *BFile) writeDatat(key []byte, value []byte) (entry, error) {
	// 1. write into datafile
	timeStamp := uint32(time.Now().Unix())
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	vec := encodeEntry(timeStamp, keySize, valueSize, key, value)
	entrySize := HeaderSize + keySize + valueSize

	valueOffset := bf.writeOffset + uint64(HeaderSize+keySize)
	// write data file into disk
	// TODO
	// assert WriteAt function
	_, err := appendWriteFile(bf.fp, vec)
	if err != nil {
		panic(err)
	}
	//logger.Debug("has write into data file:", n)

	// 2. write idx file disk
	idxData := EncodeIdx(timeStamp, keySize, valueSize, valueOffset, key)
	// TODO
	// assert write function
	_, err = appendWriteFile(bf.idxFp, idxData)
	if err != nil {
		panic(err)
	}
	//logger.Debug("has write into idx file:", n)
	bf.writeOffset += uint64(entrySize)

	return entry{
		FileID:      bf.fileID,
		ValueSize:   valueSize,
		ValueOffset: valueOffset,
		Timestamp:   timeStamp,
	}, nil
}

func (bf *BFile) del(key []byte) error {
	// 1. write into datafile
	timeStamp := uint32(time.Now().Unix())
	keySize := uint32(0)
	valueSize := uint32(0)
	vec := encodeEntry(timeStamp, keySize, valueSize, key, nil)
	//logger.Info(len(vec), keySize, valueSize)
	entrySize := HeaderSize + keySize + valueSize
	// TODO
	// race data
	valueOffset := bf.writeOffset + uint64(HeaderSize+keySize)
	// write data file into disk
	// TODO
	// assert WriteAt function
	_, err := appendWriteFile(bf.fp, vec)
	if err != nil {
		panic(err)
	}

	//logger.Debug("has write into data file:", n)
	// 2. write idx file disk
	idxData := EncodeIdx(timeStamp, keySize, valueSize, valueOffset, key)

	// TODO
	// assert write function
	_, err = appendWriteFile(bf.idxFp, idxData)
	if err != nil {
		panic(err)
	}
	//logger.Debug("has write into idx file:", n)
	bf.writeOffset += uint64(entrySize)

	return nil
}
