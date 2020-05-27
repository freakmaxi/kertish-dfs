package common

import (
	"fmt"
	"strings"
)

type SyncFileItem struct {
	Sha512Hex string
	Size      int32
}

type SyncFileItemMap map[string]SyncFileItem
type SyncFileItemList []SyncFileItem

func (f SyncFileItemMap) String() string {
	lines := make([]string, 0)
	for _, fileItem := range f {
		lines = append(lines, fmt.Sprintf("sha512Hex: %s, size: %d", fileItem.Sha512Hex, fileItem.Size))
	}
	return strings.Join(lines, "\n")
}

func Compare(fileItem1 SyncFileItem, fileItem2 SyncFileItem) bool {
	return strings.Compare(fileItem1.Sha512Hex, fileItem2.Sha512Hex) == 0 && fileItem1.Size == fileItem2.Size
}
