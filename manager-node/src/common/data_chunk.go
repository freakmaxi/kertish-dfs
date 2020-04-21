package common

type DataChunk struct {
	Sequence uint16 `json:"sequence"`
	Size     uint32 `json:"size"`
	Hash     string `json:"hash"`
}

type DataChunks []*DataChunk

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
