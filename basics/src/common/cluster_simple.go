package common

type ClusterSimple struct {
	Id    string   `json:"clusterId"`
	Nodes NodeList `json:"nodes"`
}
