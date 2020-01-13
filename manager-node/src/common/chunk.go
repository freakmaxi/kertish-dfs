package common

type Chunk struct {
	Sequence uint16 `json:"sequence"`
	Index    uint64 `json:"index"`
	Size     uint32 `json:"size"`
}
