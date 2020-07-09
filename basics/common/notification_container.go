package common

type NotificationContainer struct {
	Create       bool         `json:"create"`
	FileItem     SyncFileItem `json:"fileItems"`
	ResponseChan chan bool    `json:"-"`
}

type NotificationContainerList []*NotificationContainer

func (n NotificationContainerList) ExportFileItemList() SyncFileItemList {
	fileItemList := make(SyncFileItemList, 0)
	for _, notificationContainer := range n {
		fileItemList = append(fileItemList, notificationContainer.FileItem)
	}
	return fileItemList
}
