package common

import (
	"time"
)

type Folder struct {
	Full     string        `json:"full"`
	Name     string        `json:"name"`
	Created  time.Time     `json:"created"`
	Modified time.Time     `json:"modified"`
	Folders  FolderShadows `json:"folders"`
	Files    Files         `json:"files"`
	Size     uint64        `json:"size"`
}
