package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	MaxFileSize = 1 << 26 // 64MB

	HeaderSize = 16 // crc(4) + timestamp(8) + key_size(4)
	Magic      = byte(0x10)
)

type Store struct {
	dir      string
	active   *os.File
	fileList map[uint64]*os.File
}

func NewStore(dir string) (*Store, error) {

	var err error

	if _, err = PathExists(dir); err != nil {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, err
		}
	}

	store := &Store{
		dir:      dir,
		active:   nil,
		fileList: map[uint64]*os.File{},
	}

	err = store.createActiveFile()
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (store *Store) Open() error {
	// 扫描存储目录下的所有数据文件，并将它们添加到Store的文件列表中
	return store.AddFile()
}

func (store *Store) createActiveFile() error {

	// 如果当前已经有一个 Active 文件，则继续使用，没有则创建
	activeFileName := filepath.Join(store.dir, "000000000-000000000.bsm")
	file, err := os.OpenFile(activeFileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	file.Write([]byte{Magic}) // Write Magic to file header
	file.Sync()
	// 添加为活跃文件
	store.active = file

	if store.fileList[0] == nil {
		store.fileList[0] = file
	}

	return nil
}

func (store *Store) RenameActiveFile() error {

	oldFileName := filepath.Join(store.dir, "000000000-000000000.bsm")
	newFileName := filepath.Join(store.dir, fmt.Sprintf("000000000-%09d.bsm", store.GetFileID()))
	err := os.Rename(oldFileName, newFileName)
	if err != nil {
		return err
	}

	// 创建新的活跃文件
	store.createActiveFile()
	return nil

	// 更新 Active 文件和文件列表
	err = store.AddFile()
	if err != nil {
		return err
	}

	return err
}

func (store *Store) GetFileID() uint64 {
	var maxFileId uint64
	for fileID := range store.fileList {
		if fileID > maxFileId {
			maxFileId = fileID
		}
	}
	return maxFileId + 1

}

func (store *Store) AddFile() error {
	err := filepath.Walk(store.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".bsm" {
			return nil
		}
		file, err := os.OpenFile(path, os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		if file.Name() != "000000000-000000000.bsm" {
			fileId := store.GetFileIDByName(file)
			if store.fileList[fileId] != nil {
				store.fileList[fileId] = file
			}
		}
		return nil
	})

	return err
}

func (store *Store) GetFileIDByName(file *os.File) uint64 {
	info, err := file.Stat()
	if err != nil {
		fmt.Errorf("无效文件")
	}
	fileIDStr := strings.Split(info.Name(), "-")[1]
	fileID, err := strconv.ParseUint(fileIDStr, 10, 32)
	if err != nil {
		return 0
	}

	return fileID
}

func (store *Store) Close() error {
	var err error
	for _, file := range store.fileList {
		err = file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) Write(data []byte) (int, error) {
	if store.active == nil {
		return 0, errors.New("no active file")
	}

	// 将数据写入 Active 文件
	n, err := store.active.Write(data)
	if err != nil {
		return n, err
	}

	// 如果 Active 文件达到了一定大小，则将其重命名
	if fileInfo, err := store.active.Stat(); err == nil {
		if fileInfo.Size() > MaxFileSize {
			err = store.RenameActiveFile()
			if err != nil {
				return n, err
			}
		}
	}

	return n, nil
}

func (store *Store) Read(fileID uint32, offset int64, vsz int) (*DataEntry, error) {

	entry, err := store.readData(fileID, offset, vsz)
	if err != nil {
		return &DataEntry{}, err
	}

	return entry, nil
}

func (store *Store) readData(fileID uint32, offset int64, size int) (*DataEntry, error) {
	fileName := filepath.Join(store.dir, fmt.Sprintf("000000000-%09d.bsm", fileID))
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make([]byte, size)
	_, err = file.ReadAt(data, offset)
	if err != nil {
		return nil, err
	}

	return DecodeEntry(data)
}

func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
