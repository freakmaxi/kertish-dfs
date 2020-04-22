package common

type ClusterMap struct {
	Id      string `json:"clusterId"`
	Address string `json:"address"`
	Chunk   Chunk  `json:"chunk"`
}
