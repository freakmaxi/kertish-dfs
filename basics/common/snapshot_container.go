package common

import (
	"time"
)

type SnapshotContainer struct {
	Date      time.Time
	FileItems SyncFileItemMap
}

type SnapshotContainers []*SnapshotContainer

func (c SnapshotContainers) Len() int           { return len(c) }
func (c SnapshotContainers) Less(i, j int) bool { return c[i].Date.Before(c[j].Date) }
func (c SnapshotContainers) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

type Snapshots []time.Time

func (s Snapshots) Len() int           { return len(s) }
func (s Snapshots) Less(i, j int) bool { return s[i].Before(s[j]) }
func (s Snapshots) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func NewContainer(date time.Time) *SnapshotContainer {
	return &SnapshotContainer{
		Date: date,
	}
}
