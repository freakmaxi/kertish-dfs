package common

import "time"

const fileLockDuration = time.Hour
const defaultTransferSpeed = 1024 * 512

type FileLock struct {
	Till time.Time `json:"till"`
}

func NewFileLock(duration time.Duration) *FileLock {
	if duration == 0 {
		duration = fileLockDuration
	}
	return &FileLock{Till: time.Now().UTC().Add(duration)}
}

func NewFileLockForSize(size uint64) *FileLock {
	size /= defaultTransferSpeed
	if size < 60 {
		size = 60 // seconds
	}
	return NewFileLock(time.Second * time.Duration(size))
}

func (f *FileLock) Cancel() {
	f.Till = time.Now().UTC()
}
