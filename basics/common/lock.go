package common

import "time"

const fileLockDuration = time.Hour
const defaultTransferSpeed = 1024 * 512

// FileLock struct is to hold the file locking details related to the file
type FileLock struct {
	Till time.Time `json:"till"`
}

// NewFileLock creates a lock for file
func NewFileLock(duration time.Duration) *FileLock {
	if duration == 0 {
		duration = fileLockDuration
	}
	return &FileLock{Till: time.Now().UTC().Add(duration)}
}

// NewFileLockForSize creates a lock for the base of a file size and transfer speed calculations
func NewFileLockForSize(size uint64) *FileLock {
	size /= defaultTransferSpeed
	if size < 60 {
		size = 60 // seconds
	}
	return NewFileLock(time.Second * time.Duration(size))
}

// Cancel cancels or expires the file lock and release it
func (f *FileLock) Cancel() {
	f.Till = time.Now().UTC()
}
