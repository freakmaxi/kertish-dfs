package common

type CreationResult struct {
	Checksum string
	Chunks   DataChunks
}

func NewCreationResult() CreationResult {
	return CreationResult{
		Checksum: EmptyChecksum(),
		Chunks:   make(DataChunks, 0),
	}
}
