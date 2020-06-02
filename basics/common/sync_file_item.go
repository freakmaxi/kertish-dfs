package common

import "strings"

type SyncFileItem struct {
	Sha512Hex string
	Usage     uint16
	Size      int32
}

type SyncFileItemList []SyncFileItem
type SyncFileItemMap map[string]SyncFileItem

func (s SyncFileItemMap) ToList() SyncFileItemList {
	fileItemList := make(SyncFileItemList, 0)
	for _, fileItem := range s {
		fileItemList = append(fileItemList, fileItem)
	}
	return fileItemList
}

func CompareFileItems(fileItem1 SyncFileItem, fileItem2 SyncFileItem) bool {
	return strings.Compare(fileItem1.Sha512Hex, fileItem2.Sha512Hex) == 0 && fileItem1.Usage == fileItem2.Usage && fileItem1.Size == fileItem2.Size
}
