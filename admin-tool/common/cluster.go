package common

type Cluster struct {
	Id    string   `json:"clusterId"`
	Nodes NodeList `json:"nodes"`
}
