package common

import "time"

type Snapshots []time.Time

func (s Snapshots) Len() int           { return len(s) }
func (s Snapshots) Less(i, j int) bool { return s[i].Before(s[j]) }
func (s Snapshots) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
