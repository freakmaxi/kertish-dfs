package common

type Node struct {
	Id      string `json:"nodeId"`
	Address string `json:"address"`
	Master  bool   `json:"master"`
	Quality int64  `json:"quality"`
}

type NodeList []*Node

func (n NodeList) Len() int           { return len(n) }
func (n NodeList) Less(i, _ int) bool { return n[i].Master }
func (n NodeList) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
