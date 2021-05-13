package common

// DeletionResult struct is to hold the file deletion details applied to the dfs farm
// Untouched is the File Chunk Hash that wasn't got any action
// Deleted is the File Chunk Hash that is deleted from the dfs farm
// Missing is the File Chunk Hash that is not found or not indexed and may appear after the dfs farm sync/repair
type DeletionResult struct {
	Untouched []string
	Deleted   []string
	Missing   []string
}

// NewDeletionResult initialises the new empty DeletionResult struct
func NewDeletionResult() DeletionResult {
	return DeletionResult{
		Untouched: make([]string, 0),
		Deleted:   make([]string, 0),
		Missing:   make([]string, 0),
	}
}
