package common

import "sort"

type SyncContainer struct {
	FileItems SyncFileItemMap
	Snapshots Snapshots
}

func NewSyncContainer() *SyncContainer {
	return &SyncContainer{
		FileItems: make(SyncFileItemMap),
		Snapshots: make(Snapshots, 0),
	}
}

func (c *SyncContainer) Sort() {
	sort.Sort(c.Snapshots)
}
