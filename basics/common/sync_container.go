package common

import "sort"

// SyncContainer struct holds the data node block file sync details of cluster
type SyncContainer struct {
	FileItems SyncFileItemMap
	Snapshots Snapshots
}

// NewSyncContainer creates an instance of SyncContainer
func NewSyncContainer() *SyncContainer {
	return &SyncContainer{
		FileItems: make(SyncFileItemMap),
		Snapshots: make(Snapshots, 0),
	}
}

// Sort sorts the snapshots base on taken date
func (c *SyncContainer) Sort() {
	sort.Sort(c.Snapshots)
}
