package common

// Node struct is to hold the node details of the dfs cluster
type Node struct {
	Id      string `json:"nodeId"`
	Address string `json:"address"`
	Master  bool   `json:"master"`
	Quality int64  `json:"quality"`
}

// NodeList is the definition of the pointer array of Node struct
type NodeList []*Node

func (n NodeList) Len() int           { return len(n) }
func (n NodeList) Less(i, _ int) bool { return n[i].Master }
func (n NodeList) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }

// PrioritizedHighQualityNodeList is the definition of the pointer array of Node struct
type PrioritizedHighQualityNodeList []*Node

func (n PrioritizedHighQualityNodeList) Len() int           { return len(n) }
func (n PrioritizedHighQualityNodeList) Less(i, j int) bool { return n[i].Quality < n[j].Quality }
func (n PrioritizedHighQualityNodeList) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
