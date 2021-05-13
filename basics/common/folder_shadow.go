package common

import (
	"strings"
	"time"
)

// FolderShadow struct is to hold the virtual sub folder details
type FolderShadow struct {
	Full    string    `json:"full"`
	Name    string    `json:"name"`
	Created time.Time `json:"created"`
	Size    uint64    `json:"size" bson:"-"`
}

// FolderShadows is the definition of the pointer array of FolderShadow struct
type FolderShadows []*FolderShadow

func (f FolderShadows) Len() int           { return len(f) }
func (f FolderShadows) Less(i, j int) bool { return strings.Compare(f[i].Name, f[j].Name) < 0 }
func (f FolderShadows) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }

// NewFolderShadow creates an empty FolderShadow struct base on the provided folderPath
func NewFolderShadow(folderPath string) *FolderShadow {
	folderPath = CorrectPath(folderPath)
	_, name := Split(folderPath)

	return &FolderShadow{
		Full:    folderPath,
		Name:    name,
		Created: time.Now().UTC(),
		Size:    0,
	}
}
