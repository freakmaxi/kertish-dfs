package common

import (
	"strings"
	"time"
)

type FileSimple struct {
	Name     string    `json:"name"`
	Mime     string    `json:"mime"`
	Size     uint64    `json:"size"`
	Created  time.Time `json:"created"`
	Modified time.Time `json:"modified"`
	Locked   bool      `json:"locked"`
	Zombie   bool      `json:"zombie"`
}

type Files []*FileSimple

func (f Files) Len() int           { return len(f) }
func (f Files) Less(i, j int) bool { return strings.Compare(f[i].Name, f[j].Name) < 0 }
func (f Files) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
