package hooks

import "time"

// ActionInfo struct holds the action details that should be used by the Action provider
type ActionInfo struct {
	Time        time.Time `json:"time"`
	Action      string    `json:"action"`               // created, copied, moved, deleted
	SourcePath  string    `json:"sourcePath"`           // full path of the source file/folder that took action
	TargetPath  *string   `json:"targetPath,omitempty"` // full path of the target file/folder that took action (only copy, move)
	Folder      bool      `json:"folder"`               // path is a folder or not
	Overwritten bool      `json:"overwritten"`          // path is a file and it was overwritten
}

func NewActionInfoForCreated(createdPath string, folder bool) *ActionInfo {
	return &ActionInfo{
		Time:        time.Now().UTC(),
		Action:      "created",
		SourcePath:  createdPath,
		Folder:      folder,
		Overwritten: false,
	}
}

func NewActionInfoForCopiedFolder(sourcePath string, targetPath string) *ActionInfo {
	return &ActionInfo{
		Time:        time.Now().UTC(),
		Action:      "copied",
		SourcePath:  sourcePath,
		TargetPath:  &targetPath,
		Folder:      true,
		Overwritten: false,
	}
}

func NewActionInfoForMovedFolder(sourcePath string, targetPath string) *ActionInfo {
	return &ActionInfo{
		Time:        time.Now().UTC(),
		Action:      "moved",
		SourcePath:  sourcePath,
		TargetPath:  &targetPath,
		Folder:      true,
		Overwritten: false,
	}
}

func NewActionInfoForCopiedFile(sourcePath string, targetPath string, overwritten bool) *ActionInfo {
	return &ActionInfo{
		Time:        time.Now().UTC(),
		Action:      "copied",
		SourcePath:  sourcePath,
		TargetPath:  &targetPath,
		Folder:      false,
		Overwritten: overwritten,
	}
}

func NewActionInfoForMovedFile(sourcePath string, targetPath string, overwritten bool) *ActionInfo {
	return &ActionInfo{
		Time:        time.Now().UTC(),
		Action:      "moved",
		SourcePath:  sourcePath,
		TargetPath:  &targetPath,
		Folder:      false,
		Overwritten: overwritten,
	}
}

func NewActionInfoForDeleted(deletedPath string, folder bool) *ActionInfo {
	return &ActionInfo{
		Time:       time.Now().UTC(),
		Action:     "deleted",
		SourcePath: deletedPath,
		Folder:     folder,
	}
}
