package common

import (
	"strings"
	"time"
)

type FolderShadow struct {
	Full    string    `json:"full"`
	Name    string    `json:"name"`
	Created time.Time `json:"created"`
}

type FolderShadows []*FolderShadow

func (f FolderShadows) Len() int           { return len(f) }
func (f FolderShadows) Less(i, j int) bool { return strings.Compare(f[i].Name, f[j].Name) < 0 }
func (f FolderShadows) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }

func NewFolderShadow(folderPath string) *FolderShadow {
	folderPath = CorrectPath(folderPath)
	_, name := Split(folderPath)

	return &FolderShadow{
		Full:    folderPath,
		Name:    name,
		Created: time.Now().UTC(),
	}
}
