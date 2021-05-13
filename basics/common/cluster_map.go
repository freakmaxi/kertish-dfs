package common

// ClusterMap struct is to hold the Chunk information base on the cluster locations of the dfs farm
// this Chunk can be the whole File Chunk or the part of the File Chunk.
// Id is the cluster id where the File Chunk is located
// Address is the node address in the cluster that requester can reach and read the chunk
// Chunk is the information of chunk to read
type ClusterMap struct {
	Id      string `json:"clusterId"`
	Address string `json:"address"`
	Chunk   Chunk  `json:"chunk"`
}
