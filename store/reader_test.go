package store

import (
	"fmt"
	"os"
	"testing"
	"time"
)

func TestReader_Next(t *testing.T) {
	dir, err := os.MkdirTemp("", "reader_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	store, err := NewStore(dir)

	if err != nil {
		t.Fatal(err)
	}

	var key = []byte("Hello")
	var value = []byte("World")

	//data := bytes.Join([][]byte{key, value}, nil)
	entry1 := &DataEntry{
		Checksum:  0,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
	}

	var key2 = []byte("Hi")
	var value2 = []byte("World")

	//data := bytes.Join([][]byte{key, value}, nil)
	entry2 := &DataEntry{
		Checksum:  0,
		Key:       key2,
		Value:     value2,
		Timestamp: time.Now().UnixNano(),
	}

	data1, err := EncodeEntry(entry1)
	if err != nil {
		t.Fatal(err)
	}

	data2, err := EncodeEntry(entry2)
	if err != nil {
		t.Fatal(err)
	}

	store.Write(data1)
	store.Write(data2)

	reader, err := NewReader(fmt.Sprintf("%s/000000000-000000000.bsm", dir))
	if err != nil {
		t.Fatal(err)
	}
	kv, _ := reader.Next()

	if string(key) != string(kv.Key) {
		t.Fatal("expect %, got %", string(key), string(kv.Key))
	}
	if string(value) != string(kv.Value) {
		t.Fatal("expect %, got %", string(value), string(kv.Value))
	}
	kv, _ = reader.Next()

	if string(key2) != string(kv.Key) {
		t.Fatal("expect %, got %", string(key), string(kv.Key))
	}

	if string(value2) != string(kv.Value) {
		t.Fatal("expect %, got %", string(value2), string(kv.Value))
	}
}
