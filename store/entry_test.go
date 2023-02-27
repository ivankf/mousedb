package store

import (
	"bytes"
	"testing"
	"time"
)

func TestNewEntry(t *testing.T) {
	key := []byte("testKey")
	value := []byte("testValue")
	ts := time.Now()

	entry := NewEntry(key, value, &ts)

	if !bytes.Equal(entry.Key, key) {
		t.Errorf("Expected key %v, but got %v", key, entry.Key)
	}
	if !bytes.Equal(entry.Value, value) {
		t.Errorf("Expected value %v, but got %v", value, entry.Value)
	}
	if entry.Timestamp != ts.UnixNano() {
		t.Errorf("Expected timestamp %v, but got %v", ts.UnixNano(), entry.Timestamp)
	}
}

func TestEncodeDecodeEntry(t *testing.T) {
	key := []byte("testKey")
	value := []byte("testValue")
	ts := time.Now()

	entry := NewEntry(key, value, &ts)

	data, err := EncodeEntry(&entry)
	if err != nil {
		t.Errorf("Unexpected error encoding entry: %v", err)
	}

	decodedEntry, err := DecodeEntry(data)
	if err != nil {
		t.Errorf("Unexpected error decoding entry: %v", err)
	}

	if !bytes.Equal(decodedEntry.Key, key) {
		t.Errorf("Expected key %v, but got %v", key, decodedEntry.Key)
	}
	if !bytes.Equal(decodedEntry.Value, value) {
		t.Errorf("Expected value %v, but got %v", value, decodedEntry.Value)
	}
	if decodedEntry.Timestamp != ts.UnixNano() {
		t.Errorf("Expected timestamp %v, but got %v", ts.UnixNano(), decodedEntry.Timestamp)
	}
}
