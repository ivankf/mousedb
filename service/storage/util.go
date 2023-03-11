package storage

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	lockFileName = "mousedb.lock"
)

// if writeableFile size large than Opts.MaxFileSize and the fileID not equal to local time stamp;
// if will create a new writeable file
func checkWriteableFile(storage *Storage) {
	if storage.writeFile.writeOffset > storage.Config.MaxFileSize && storage.writeFile.fileID != uint32(time.Now().Unix()) {
		storage.Logger.Info(fmt.Sprintf("open a new data/idx file: %d, %d", storage.writeFile.writeOffset, storage.Config.MaxFileSize))
		//close data/idx fp
		storage.writeFile.idxFp.Close()
		storage.writeFile.fp.Close()

		writeFp, fileID := setWriteableFile(0, storage.dirFile)
		idxFp := setIdxFile(fileID, storage.dirFile)
		bf := &BFile{
			fp:          writeFp,
			fileID:      fileID,
			writeOffset: 0,
			idxFp:       idxFp,
		}
		storage.writeFile = bf
		// update pid
		writePID(storage.lockFile, fileID)
	}
}

// return the idx file lists
func listIdxFiles(storage *Storage) ([]string, error) {
	filterFiles := []string{lockFileName}
	dirFp, err := os.OpenFile(storage.dirFile, os.O_RDONLY, os.ModeDir)
	if err != nil {
		return nil, err
	}
	defer dirFp.Close()
	//
	lists, err := dirFp.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	var idxLists []string
	for _, v := range lists {
		if strings.Contains(v, "idx") && !existsSuffixs(filterFiles, v) {
			idxLists = append(idxLists, v)
		}
	}
	return idxLists, nil
}

// return the data file lists
func listDataFiles(storage *Storage) ([]string, error) {
	filterFiles := []string{lockFileName}
	dirFp, err := os.OpenFile(storage.dirFile, os.O_RDONLY, os.ModeDir)
	if err != nil {
		return nil, err
	}
	defer dirFp.Close()
	//
	lists, err := dirFp.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	var dataFileLists []string
	for _, v := range lists {
		if strings.Contains(v, BSM) && !existsSuffixs(filterFiles, v) {
			dataFileLists = append(dataFileLists, v)
		}
	}
	sort.Strings(dataFileLists)
	return dataFileLists, nil
}

// lock a file by fp locker; the file must exits
func lockFile(fileName string) (*os.File, error) {
	return os.OpenFile(fileName, os.O_EXCL|os.O_CREATE|os.O_RDWR, os.ModePerm)
}

func existsSuffixs(suffixs []string, src string) (b bool) {
	for _, suffix := range suffixs {
		if b = strings.HasSuffix(src, suffix); b {
			return
		}
	}
	return
}

func writePID(pidFp *os.File, fileID uint32) {
	pidFp.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\t"+strconv.Itoa(int(fileID))+BSM), 0)
}

// get file last idx file info
func lastFileInfo(files []*os.File) (uint32, *os.File) {
	if files == nil {
		return uint32(0), nil
	}
	lastFp := files[0]

	fileName := lastFp.Name()
	s := strings.LastIndex(fileName, "/") + 1
	e := strings.LastIndex(fileName, IDX)
	idx, _ := strconv.Atoi(fileName[s:e])
	lastID := idx
	for i := 0; i < len(files); i++ {
		idxFp := files[i]
		fileName = idxFp.Name()
		s = strings.LastIndex(fileName, "/") + 1
		e = strings.LastIndex(fileName, IDX)
		idx, _ = strconv.Atoi(fileName[s:e])
		if lastID < idx {
			lastID = idx
			lastFp = idxFp
		}
	}
	return uint32(lastID), lastFp
}

func closeReadIdxFp(files []*os.File, fileID uint32) {
	for _, fp := range files {
		if !strings.Contains(fp.Name(), strconv.Itoa(int(fileID))) {
			fp.Close()
		}
	}
}

func setWriteableFile(fileID uint32, dirName string) (*os.File, uint32) {
	var fp *os.File
	var err error
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fileName := dirName + "/" + strconv.Itoa(int(fileID)) + BSM
	fp, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}
	return fp, fileID
}

func setIdxFile(fileID uint32, dirName string) *os.File {
	var fp *os.File
	var err error
	if fileID == 0 {
		fileID = uint32(time.Now().Unix())
	}
	fileName := dirName + "/" + strconv.Itoa(int(fileID)) + IDX
	fp, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		panic(err)
	}
	return fp
}

func appendWriteFile(fp *os.File, buf []byte) (int, error) {
	stat, err := fp.Stat()
	if err != nil {
		return -1, err
	}

	return fp.WriteAt(buf, stat.Size())
}

// return a unique not exists file name by timeStamp
func uniqueFileName(root, suffix string) string {
	for {
		tStamp := strconv.Itoa(int(time.Now().Unix()))
		_, err := os.Stat(root + "/" + tStamp + "." + suffix)
		if err != nil && os.IsNotExist(err) {
			return tStamp + "." + suffix
		}
		time.Sleep(time.Second)
	}
}
