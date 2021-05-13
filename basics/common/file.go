package common

import (
	"crypto/md5"
	"encoding/hex"
	"sort"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/errors"
)

// File struct is to hold the actual file details in dfs cluster
type File struct {
	Name     string     `json:"name"`
	Mime     string     `json:"mime"`
	Size     uint64     `json:"size"`
	Checksum string     `json:"checksum"`
	Created  time.Time  `json:"created"`
	Modified time.Time  `json:"modified"`
	Chunks   DataChunks `json:"chunks"`
	Missing  DataChunks `json:"missing"`
	Lock     *FileLock  `json:"lock"`
	Zombie   bool       `json:"zombie"`
}

// Files is the definition of the pointer array of File struct
type Files []*File

func (f Files) Len() int           { return len(f) }
func (f Files) Less(i, j int) bool { return strings.Compare(f[i].Name, f[j].Name) < 0 }
func (f Files) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }

// CreateJoinedFile creates a file struct from multiple files and behave like a single file
func CreateJoinedFile(files Files) (*File, error) {
	hash := md5.New()

	mime := ""
	sequenceCount := uint16(0)
	joinedFile := newFile("")
	for _, f := range files {
		if f.Locked() {
			return nil, errors.ErrLock
		}
		if f.ZombieCheck() {
			return nil, errors.ErrZombie
		}
		if _, err := hash.Write([]byte(f.Name)); err != nil {
			return nil, err
		}
		joinedFile.Size += f.Size

		sort.Sort(f.Chunks)
		for _, c := range f.Chunks {
			shadow := *c

			shadow.Sequence = sequenceCount
			sequenceCount++

			joinedFile.Chunks = append(joinedFile.Chunks, &shadow)
		}

		if len(mime) == 0 {
			mime = f.Mime
			continue
		}
		if strings.Compare(mime, f.Mime) == 0 {
			continue
		}

		mime = joinedFile.Mime
	}
	joinedFile.Mime = mime
	joinedFile.Name = hex.EncodeToString(hash.Sum(nil))

	return joinedFile, nil
}

func newFile(name string) *File {
	return &File{
		Name:     name,
		Mime:     "application/octet-stream",
		Checksum: EmptyChecksum(),
		Size:     0,
		Created:  time.Now().UTC(),
		Modified: time.Now().UTC(),
		Chunks:   make(DataChunks, 0),
		Missing:  make(DataChunks, 0),
		Lock:     NewFileLock(0),
		Zombie:   true,
	}
}

// IngestDeletion ingest the deletion information to file struct to be able to
// track the consistent deletion operation across the dfs clusters
func (f *File) IngestDeletion(deletionResult DeletionResult) {
	if len(deletionResult.Untouched) == 0 && len(deletionResult.Deleted) == 0 && len(deletionResult.Missing) == 0 {
		return
	}

	chunks := make(DataChunks, len(f.Chunks))
	copy(chunks, f.Chunks)

	untouchedChunkHashesMap := make(map[string]bool)
	for _, untouchedChunkHash := range deletionResult.Untouched {
		untouchedChunkHashesMap[untouchedChunkHash] = true
	}

	deletedChunkHashesMap := make(map[string]bool)
	for _, deletedChunkHash := range deletionResult.Deleted {
		deletedChunkHashesMap[deletedChunkHash] = true
	}

	missingChunkHashesMap := make(map[string]bool)
	for _, missingChunkHash := range deletionResult.Missing {
		missingChunkHashesMap[missingChunkHash] = true
	}

	f.Chunks = make(DataChunks, 0)

	for i := 0; i < len(chunks); i++ {
		chunk := chunks[i]

		_, has := untouchedChunkHashesMap[chunk.Hash]
		if has {
			f.Chunks = append(f.Chunks, chunk)
			chunks = append(chunks[:i], chunks[i+1:]...)
			i--

			continue
		}

		_, has = missingChunkHashesMap[chunk.Hash]
		if has {
			f.Missing = append(f.Missing, chunk)
			chunks = append(chunks[:i], chunks[i+1:]...)
			i--

			continue
		}

		_, has = deletedChunkHashesMap[chunk.Hash]
		if !has {
			f.Missing = append(f.Missing, chunk)
		}
	}

	f.Zombie = len(f.Chunks)+len(deletionResult.Deleted) == 0 || len(f.Missing) > 0
	f.Missing = append(f.Missing, chunks...)
}

// ZombieCheck checks if the file is zombie in the dfs clustes
func (f *File) ZombieCheck() bool {
	f.Zombie = f.Zombie || len(f.Chunks) == 0
	return f.Zombie
}

// CanDie checks if the zombie file can be deleted
func (f *File) CanDie() bool {
	return f.Zombie && len(f.Chunks) == 0
}

// Resurrect resets the zombie situation of the file to able to repair
func (f *File) Resurrect() {
	if len(f.Missing) == 0 {
		return
	}

	f.Chunks = append(f.Chunks, f.Missing...)
	f.Missing = make(DataChunks, 0)
	f.Zombie = false
}

// Locked checks if file is locked for any other operation
func (f *File) Locked() bool {
	return f.Lock != nil && f.Lock.Till.After(time.Now().UTC())
}

// Reset resets the file struct fields to default values
func (f *File) Reset(mime string, size uint64) {
	f.Mime = mime
	f.Size = size
	f.Checksum = EmptyChecksum()
	f.Created = time.Now().UTC()
	f.Modified = time.Now().UTC()
	f.Chunks = make(DataChunks, 0)
	f.Missing = make(DataChunks, 0)
	f.Lock = NewFileLock(0)
	f.Zombie = false
}

// CloneInto copies the file struct in to a target file struct
func (f *File) CloneInto(target *File) {
	if target == nil {
		return
	}

	target.Mime = f.Mime
	target.Size = f.Size
	target.Checksum = f.Checksum
	target.Lock = f.Lock

	target.Chunks = make(DataChunks, 0)
	for _, c := range f.Chunks {
		shadow := *c
		target.Chunks = append(target.Chunks, &shadow)
	}
}
