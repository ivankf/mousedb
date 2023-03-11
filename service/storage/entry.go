package storage

import "fmt"

// entry represents a key-value pair in mousedb
type entry struct {
	FileID      uint32 // ID of the file containing the value
	ValueSize   uint32 // Size of the value in bytes
	ValueOffset uint64 // Offset of the value in the data block
	Timestamp   uint32 // Unix timestamp of the file access time
}

// String returns a string representation of the entry
func (e *entry) String() string {
	return fmt.Sprintf("Timestamp: %d, FileID: %d, ValueSize: %d, ValueOffset: %d", e.Timestamp,
		e.FileID, e.ValueSize, e.ValueOffset)
}

// IsNewerThan returns true if the entry is newer than the old entry
func (e *entry) IsNewerThan(old *entry) bool {
	if e.Timestamp > old.Timestamp ||
		(e.Timestamp == old.Timestamp && e.FileID > old.FileID) ||
		(e.Timestamp == old.Timestamp && e.FileID == old.FileID && e.ValueOffset > old.ValueOffset) {
		return true
	}
	return false
}

// IsEqualTo returns true if the entry is equal to the old entry
func (e *entry) IsEqualTo(old *entry) bool {
	if e.Timestamp == old.Timestamp && e.FileID == old.FileID && e.ValueOffset == old.ValueOffset {
		return true
	}
	return false
}
