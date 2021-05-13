package common

// NotificationContainer struct is to hold and export/serialize file creation or
// deletion operation notification to the farm manager
// Create is the boolean and notify if the operation was the creation. otherwise deletion.
// FileItem is the File Chunk information that took place for the the operation
// ResponseChan is just used for the internal operation sync in the data node. Not exported.
type NotificationContainer struct {
	Create       bool         `json:"create"`
	FileItem     SyncFileItem `json:"fileItems"`
	ResponseChan chan bool    `json:"-"`
}

// NotificationContainerList is the definition of the pointer array of NotificationContainer struct
type NotificationContainerList []*NotificationContainer

// ExportFileItemList exports the FileItem entries as the SyncFileItemList from the NotificationContainerList
func (n NotificationContainerList) ExportFileItemList() SyncFileItemList {
	fileItemList := make(SyncFileItemList, 0)
	for _, notificationContainer := range n {
		fileItemList = append(fileItemList, notificationContainer.FileItem)
	}
	return fileItemList
}
