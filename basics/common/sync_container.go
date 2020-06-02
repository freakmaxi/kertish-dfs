package common

import (
	"sort"
)

type SyncContainer struct {
	FileItems SyncFileItemMap
	Snapshots SnapshotContainers
}

func NewSyncContainer() *SyncContainer {
	return &SyncContainer{
		Snapshots: make(SnapshotContainers, 0),
	}
}

func (c *SyncContainer) Sort() {
	sort.Sort(c.Snapshots)
}
