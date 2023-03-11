package storage

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"
)

// ErrNotFound ...
var (
	ErrNotFound = fmt.Errorf("not Found")
	ErrIsNotDir = fmt.Errorf("the file is not dir")
)

// New a Storage service
func New(config *Config) *Storage {
	return &Storage{
		Config: config,
		Logger: zap.NewNop(),
	}
}

// WithLogger sets the logger for the store.
func (storage *Storage) WithLogger(log *zap.Logger) {
	storage.baseLogger = log
	storage.Logger = log.With(zap.String("service", "storage"))
}

func (storage *Storage) Open() error {
	if storage.Config == nil {
		storage.Config = NewConfig()
	}

	_, err := os.Stat(storage.Config.Dir)
	if err != nil && !os.IsNotExist(err) {
		return nil
	}

	if os.IsNotExist(err) {
		err = os.Mkdir(storage.Config.Dir, 0755)
		if err != nil {
			return err
		}
	}
	storage.dirFile = storage.Config.Dir
	storage.oldFile = newBFiles()
	storage.rwLock = &sync.RWMutex{}

	// lock file
	storage.lockFile, err = lockFile(storage.Config.Dir + "/" + lockFileName)
	if err != nil {
		return err
	}
	storage.entryCache = NewEntryCache()
	// scan readAble file
	files, _ := storage.readableFiles()
	storage.parseIdx(files)

	//get the last fileid
	fileId, idxFp := lastFileInfo(files)

	var writeFp *os.File
	writeFp, fileId = setWriteableFile(fileId, storage.Config.Dir)

	idxFp = setIdxFile(fileId, storage.Config.Dir)

	// close other idx
	closeReadIdxFp(files, fileId)

	// setting writeable file, only one
	dataSet, _ := writeFp.Stat()
	bf := &BFile{
		fp:          writeFp,
		fileID:      fileId,
		writeOffset: uint64(dataSet.Size()),
		idxFp:       idxFp,
	}
	storage.writeFile = bf

	// save pid into mousedb.lock file
	writePID(storage.lockFile, fileId)

	return nil
}

// Storage ...
type Storage struct {
	Logger     *zap.Logger
	baseLogger *zap.Logger

	Config     *Config       // config for Storage
	oldFile    *BFiles       // idx file, data file
	lockFile   *os.File      // lock file with process
	entryCache *EntryCache   // key/value hashMap, building with idx file
	dirFile    string        // mousedb storage  root dir
	writeFile  *BFile        // writeable file
	rwLock     *sync.RWMutex // rwlocker for mousedb Get and put Operation

}

// Close opening fp
func (storage *Storage) Close() error {
	// close ActiveFiles
	storage.oldFile.close()
	// close writeable file
	if err := storage.writeFile.fp.Close(); err != nil {
		return err
	}
	if err := storage.writeFile.idxFp.Close(); err != nil {
		return err
	}
	// close lockFile
	if err := storage.lockFile.Close(); err != nil {
		return err
	}
	// delete lockFile
	os.Remove(storage.dirFile + "/" + lockFileName)
	return nil
}

// Put key/value
func (storage *Storage) Put(key []byte, value []byte) error {
	storage.rwLock.Lock()
	defer storage.rwLock.Unlock()
	checkWriteableFile(storage)
	// write data into writeable file
	e, err := storage.writeFile.writeDatat(key, value)
	if err != nil {
		storage.rwLock.Unlock()
		return err
	}
	// add key/value into EntryCache
	storage.entryCache.Put(string(key), &e)
	return nil
}

// Get ...
func (storage *Storage) Get(key []byte) ([]byte, error) {

	e := storage.entryCache.Get(string(key))
	if e == nil {
		return nil, ErrNotFound
	}

	fileID := e.FileID
	bf, err := storage.getFileState(fileID)
	if err != nil && os.IsNotExist(err) {
		storage.Logger.Info("The key is not exits", zap.Error(err))
		return nil, err
	}

	return bf.read(e.ValueOffset, e.ValueSize)
}

// Del value by key
func (storage *Storage) Del(key []byte) error {
	storage.rwLock.Lock()
	defer storage.rwLock.Unlock()
	if storage.writeFile == nil {
		return fmt.Errorf("can Not Read The MouseDB Root Director")
	}
	e := storage.entryCache.Get(string(key))
	if e == nil {
		return ErrNotFound
	}

	checkWriteableFile(storage)
	// write data into writeable file
	err := storage.writeFile.del(key)
	if err != nil {
		return err
	}
	// delete key/value from EntryCache
	storage.entryCache.Del(string(key))
	return nil
}

// return readable idx file: xxxx.idx
func (storage *Storage) readableFiles() ([]*os.File, error) {
	filterFiles := []string{lockFileName}
	ldfs, err := listIdxFiles(storage)
	if err != nil {
		return nil, err
	}

	fps := make([]*os.File, 0, len(ldfs))
	for _, filePath := range ldfs {
		if existsSuffixs(filterFiles, filePath) {
			continue
		}
		fp, err := os.OpenFile(storage.dirFile+"/"+filePath, os.O_RDONLY, 0755)
		if err != nil {
			return nil, err
		}
		fps = append(fps, fp)
	}
	if len(fps) == 0 {
		return nil, nil
	}
	return fps, nil
}

func (storage *Storage) getFileState(fileID uint32) (*BFile, error) {
	// lock up it from write able file
	if fileID == storage.writeFile.fileID {
		return storage.writeFile, nil
	}
	// if not exits in write able file, look up it from OldFile
	bf := storage.oldFile.get(fileID)
	if bf != nil {
		return bf, nil
	}

	bf, err := openBFile(storage.dirFile, int(fileID))
	if err != nil {
		return nil, err
	}
	storage.oldFile.put(bf, fileID)
	return bf, nil
}

func (storage *Storage) parseIdx(idxFps []*os.File) {

	b := make([]byte, IdxHeaderSize, IdxHeaderSize)
	for _, fp := range idxFps {
		offset := int64(0)
		idxName := fp.Name()
		s := strings.LastIndex(idxName, "/") + 1
		e := strings.LastIndex(idxName, IDX)
		fileID, _ := strconv.ParseInt(idxName[s:e], 10, 32)

		for {
			// parse idx header
			n, err := fp.ReadAt(b, offset)
			offset += int64(n)
			if err != nil && err != io.EOF {
				panic(err)
			}
			if err == io.EOF {
				break
			}

			if n != IdxHeaderSize {
				panic(n)
			}

			timestamp, ksz, valueSz, valuePos := DecodeIdx(b)
			if ksz+valueSz == 0 { // the record is deleted
				continue
			}

			// parse idx key
			keyByte := make([]byte, ksz)
			n, err = fp.ReadAt(keyByte, offset)
			if err != nil && err != io.EOF {
				panic(err)
			}
			if err == io.EOF {
				break
			}
			if n != int(ksz) {
				panic(n)
			}
			key := string(keyByte)

			e := &entry{
				FileID:      uint32(fileID),
				ValueSize:   valueSz,
				ValueOffset: valuePos,
				Timestamp:   timestamp,
			}
			offset += int64(ksz)
			// put entry into EntryCache
			storage.entryCache.Put(key, e)
		}
	}
}
