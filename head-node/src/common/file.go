package common

import (
	"crypto/md5"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

type File struct {
	Name     string     `json:"name"`
	Mime     string     `json:"mime"`
	Size     uint64     `json:"size"`
	Created  time.Time  `json:"created"`
	Modified time.Time  `json:"modified"`
	Chunks   DataChunks `json:"chunks"`
	Locked   bool       `json:"locked"`
}

type Files []*File

func (f Files) Len() int           { return len(f) }
func (f Files) Less(i, j int) bool { return strings.Compare(f[i].Name, f[j].Name) < 0 }
func (f Files) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }

func CreateJoinedFile(files Files) (*File, error) {
	hash := md5.New()

	mime := ""
	sequenceCount := uint16(0)
	joinedFile := newFile("")
	for _, f := range files {
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
		Size:     0,
		Created:  time.Now().UTC(),
		Modified: time.Now().UTC(),
		Chunks:   make(DataChunks, 0),
		Locked:   true,
	}
}

func (f *File) Reset(mime string, size uint64) {
	f.Mime = mime
	f.Size = size
	f.Created = time.Now().UTC()
	f.Modified = time.Now().UTC()
	f.Chunks = make(DataChunks, 0)
	f.Locked = true
}

func (f *File) CloneInto(target *File) {
	if target == nil {
		return
	}

	target.Mime = f.Mime
	target.Size = f.Size
	target.Locked = f.Locked

	target.Chunks = make(DataChunks, 0)
	for _, c := range f.Chunks {
		shadow := *c
		target.Chunks = append(target.Chunks, &shadow)
	}
}
