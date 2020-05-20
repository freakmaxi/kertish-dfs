package common

type SyncDelete struct {
	Sha512Hex string `json:"sha512Hex"`
	Shadow    bool   `json:"shadow"`
	Size      uint32 `json:"size"`
}

type SyncDeleteList []SyncDelete

func (s SyncDeleteList) Size() uint64 {
	total := uint64(0)
	for _, syncDelete := range s {
		total += uint64(syncDelete.Size)
	}
	return total
}

func (s SyncDeleteList) Wiped() []string {
	wiped := make([]string, 0)
	for _, syncDelete := range s {
		if syncDelete.Shadow {
			continue
		}
		wiped = append(wiped, syncDelete.Sha512Hex)
	}
	return wiped
}
