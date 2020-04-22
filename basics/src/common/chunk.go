package common

type Chunk struct {
	Sequence uint16 `json:"sequence"`
	Index    uint64 `json:"index"`
	Size     uint32 `json:"size"`
}

func (c Chunk) Starts() uint64 {
	return c.Index
}

func (c Chunk) Ends() uint64 {
	return c.Index + uint64(c.Size)
}
