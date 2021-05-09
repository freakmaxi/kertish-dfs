package common

import "time"

type TreeShadow struct {
	Full     string      `json:"full"`
	Name     string      `json:"name"`
	Created  time.Time   `json:"created"`
	Modified time.Time   `json:"modified"`
	Size     uint64      `json:"size"`
	Folders  TreeShadows `json:"folders"`
}

type TreeShadows []*TreeShadow
