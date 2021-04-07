package common

import "strings"

type SyncFileItem struct {
	Sha512Hex string `json:"sha512Hex"`
	Usage     uint16 `json:"usage"`
	Size      uint32 `json:"size"`
	Shadow    bool   `json:"shadow"`
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

func (s SyncFileItemList) ShadowItems() SyncFileItemList {
	shadowFileItemList := make(SyncFileItemList, 0)
	for _, fileItem := range s {
		if !fileItem.Shadow {
			continue
		}
		shadowFileItemList = append(shadowFileItemList, fileItem)
	}
	return shadowFileItemList
}

func (s SyncFileItemList) PhysicalSize() uint64 {
	total := uint64(0)
	for _, fileItem := range s {
		if fileItem.Shadow {
			continue
		}
		total += uint64(fileItem.Size)
	}
	return total
}

func CompareFileItems(fileItem1 SyncFileItem, fileItem2 SyncFileItem) bool {
	return strings.Compare(fileItem1.Sha512Hex, fileItem2.Sha512Hex) == 0 && fileItem1.Usage == fileItem2.Usage && fileItem1.Size == fileItem2.Size
}
