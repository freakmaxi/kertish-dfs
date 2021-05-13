package common

// DataChunk struct is to hold the block File particle information
type DataChunk struct {
	Sequence uint16 `json:"sequence"`
	Size     uint32 `json:"size"`
	Hash     string `json:"hash"`
}

// DataChunks is the definition of the pointer array of DataChunk struct
type DataChunks []*DataChunk

// NewDataChunk initialises a new DataChunk using the given information
func NewDataChunk(sequence uint16, size uint32, sha512 string) *DataChunk {
	return &DataChunk{
		Sequence: sequence,
		Size:     size,
		Hash:     sha512,
	}
}

func (d DataChunks) Len() int           { return len(d) }
func (d DataChunks) Less(i, j int) bool { return d[i].Sequence < d[j].Sequence }
func (d DataChunks) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
