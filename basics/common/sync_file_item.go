package common

import "strings"

// SyncFileItem struct is to hold and export/serialize the file creation/deletion/sync operation across the dfs farm
type SyncFileItem struct {
	Sha512Hex string `json:"sha512Hex"`
	Usage     uint16 `json:"usage"`
	Size      uint32 `json:"size"`
	Shadow    bool   `json:"shadow"`
}

// SyncFileItemList is the definition of the array of SyncFileItem struct
type SyncFileItemList []SyncFileItem

// SyncFileItemMap is the map definition of SyncFileItem struct base on Sha512Hex key for the O(1) reachability
type SyncFileItemMap map[string]SyncFileItem

// ToList exports the map entries to SyncFileItemList
func (s SyncFileItemMap) ToList() SyncFileItemList {
	fileItemList := make(SyncFileItemList, 0)
	for _, fileItem := range s {
		fileItemList = append(fileItemList, fileItem)
	}
	return fileItemList
}

// ShadowItems filters the map entries that are only marked as Shadow
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

// PhysicalSize calculates the total file size (ignoring the shadow entries) in the SyncFileItemList
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

// CompareFileItems compares two SyncFileItems to check if they are equal or not.
func CompareFileItems(fileItem1 SyncFileItem, fileItem2 SyncFileItem) bool {
	return strings.Compare(fileItem1.Sha512Hex, fileItem2.Sha512Hex) == 0 && fileItem1.Usage == fileItem2.Usage && fileItem1.Size == fileItem2.Size
}
