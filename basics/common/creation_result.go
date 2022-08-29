package common

// CreationResult struct is to hold the file creation details in the dfs farm
// Checksum is the whole file checksum
// Chunks is the list of the whole File particles
type CreationResult struct {
	Checksum string
	Chunks   DataChunks
}

// NewCreationResult initialises the new empty CreationResult struct
func NewCreationResult(sha512Hex string, chunks DataChunks) *CreationResult {
	return &CreationResult{
		Checksum: sha512Hex,
		Chunks:   chunks,
	}
}
