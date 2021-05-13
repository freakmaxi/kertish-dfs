package common

// CreationResult struct is to hold the file creation details in the dfs farm
// Checksum is the whole file checksum
// Chunks is the list of the whole File particles
type CreationResult struct {
	Checksum string
	Chunks   DataChunks
}

// NewCreationResult initialises the new empty CreationResult struct
func NewCreationResult() CreationResult {
	return CreationResult{
		Checksum: EmptyChecksum(),
		Chunks:   make(DataChunks, 0),
	}
}
