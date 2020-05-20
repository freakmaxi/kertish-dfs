package common

import "strings"

type SyncFileItem struct {
	Sha512Hex string
	Size      int
}

type SyncFileItems []SyncFileItem

func (f SyncFileItems) Len() int { return len(f) }
func (f SyncFileItems) Less(i, j int) bool {
	return strings.Compare(f[i].Sha512Hex, f[j].Sha512Hex) < 0
}
func (f SyncFileItems) Swap(i, j int) { f[i], f[j] = f[j], f[i] }

func Compare(fileItem1 SyncFileItem, fileItem2 SyncFileItem) bool {
	return strings.Compare(fileItem1.Sha512Hex, fileItem2.Sha512Hex) == 0 && fileItem1.Size == fileItem2.Size
}
