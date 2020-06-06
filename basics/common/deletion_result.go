package common

type DeletionResult struct {
	Untouched []string
	Deleted   []string
	Missing   []string
}

func NewDeletionResult() DeletionResult {
	return DeletionResult{
		Untouched: make([]string, 0),
		Deleted:   make([]string, 0),
		Missing:   make([]string, 0),
	}
}
