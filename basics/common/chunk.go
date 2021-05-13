package common

// Chunk struct is to hold the File particle information for the reservation operation
// Sequence is the order number of the particle
// Index is the particle starting point in the whole file
// Size is the length of the particle
type Chunk struct {
	Sequence uint16 `json:"sequence"`
	Index    uint64 `json:"index"`
	Size     uint32 `json:"size"`
}

// Starts locates the starting index of the particle of the whole file
func (c Chunk) Starts() uint64 {
	return c.Index
}

// Ends calculates the ending index of the particle in the whole file
func (c Chunk) Ends() uint64 {
	return c.Index + uint64(c.Size)
}
