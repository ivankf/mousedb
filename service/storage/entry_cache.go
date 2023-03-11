package storage

import "sync"

// EntryCache for HashMap
type EntryCache struct {
	sync.RWMutex
	entries map[string]*entry
}

// NewEntryCache creates a new EntryCache object
func NewEntryCache() *EntryCache {
	return &EntryCache{
		entries: make(map[string]*entry),
	}
}

// Get retrieves the value associated with the given key
func (k *EntryCache) Get(key string) *entry {
	k.RLock()
	defer k.RUnlock()
	return k.entries[key]
}

// Del removes the entry associated with the given key
func (k *EntryCache) Del(key string) {
	k.Lock()
	defer k.Unlock()
	delete(k.entries, key)
}

// Put inserts a new key-value entry into the EntryCache
func (k *EntryCache) Put(key string, e *entry) {
	k.Lock()
	defer k.Unlock()
	k.entries[key] = e
}

// SetCompare compares the given entry with the existing entry in EntryCache and updates the value if necessary
func (k *EntryCache) SetCompare(key string, e *entry) bool {
	k.Lock()
	defer k.Unlock()
	old, ok := k.entries[key]
	if !ok || e.IsEqualTo(old) {
		k.entries[key] = e
		return true
	}
	return false
}

// UpdateFileID updates the file ID for all entries in EntryCache that have the given old ID
func (k *EntryCache) UpdateFileID(oldID, newID uint32) {
	k.Lock()
	defer k.Unlock()
	for _, e := range k.entries {
		if e.FileID == oldID {
			e.FileID = newID
		}
	}
}
