package store

import (
	"hash/crc32"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestNewStore(t *testing.T) {
	dir, err := os.MkdirTemp("", "store_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	if store == nil {
		t.Fatal("store is nil")
	}

	if store.active == nil {
		t.Fatal("active file is nil")
	}

	if len(store.fileList) != 1 {
		t.Fatalf("expected 1 file, got %d", len(store.fileList))
	}

	if store.dir != dir {
		t.Fatalf("expected dir %s, got %s", dir, store.dir)
	}
}

func TestStore_Write(t *testing.T) {
	dir, err := os.MkdirTemp("", "store_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	data := []byte("hello world")
	n, err := store.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	if n != len(data) {
		t.Fatalf("expected %d bytes to be written, got %d", len(data), n)
	}
}

func TestStore_Read(t *testing.T) {
	dir, err := os.MkdirTemp("", "store_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewStore(dir)
	if err != nil {
		t.Fatal(err)
	}

	// write some data to store
	key := "foo"
	value := "bar"
	entry := &DataEntry{
		Checksum:  crc32.ChecksumIEEE([]byte(value)),
		Key:       []byte(key),
		Value:     []byte(value),
		Timestamp: time.Now().UnixNano(),
	}

	data, err := EncodeEntry(entry)

	_, err = store.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	// read the data back
	fileID := uint32(0)
	offset := int64(0)
	size := len(data)
	readData, err := store.Read(fileID, offset, size)
	if err != nil {
		t.Fatal(err)
	}

	got, err := EncodeEntry(readData)

	if string(got) != string(data) {
		t.Fatalf("expected %s, got %s", string(data), string(got))
	}
}

func BenchmarkStoreWrite(b *testing.B) {
	dir, err := ioutil.TempDir("", "store-test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewStore(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	data := []byte("hello world")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.Write(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoreRead(b *testing.B) {
	dir, err := ioutil.TempDir("", "store-test")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := NewStore(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()

	data := []byte("hello world")
	for i := 0; i < 100; i++ {
		_, err := s.Write(data)
		if err != nil {
			b.Fatal(err)
		}
	}

	fileID := uint32(0)
	offset := int64(0)
	vsz := len(data)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := s.Read(fileID, offset, vsz)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func randStringBytes(n int) string {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
