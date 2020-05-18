package common

import (
	"sort"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/errors"
)

type Cluster struct {
	Id           string            `json:"clusterId"`
	Size         uint64            `json:"size"`
	Used         uint64            `json:"used"`
	Nodes        NodeList          `json:"nodes"`
	Reservations map[string]uint64 `json:"reservations"`
	Paralyzed    bool              `json:"paralyzed"`
	Frozen       bool              `json:"frozen"`
}

type Clusters []*Cluster

func (c Clusters) Len() int           { return len(c) }
func (c Clusters) Less(i, j int) bool { return c[i].Used < c[j].Used }
func (c Clusters) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func NewCluster(id string) *Cluster {
	return &Cluster{
		Id:           id,
		Nodes:        NodeList{},
		Reservations: make(map[string]uint64),
		Frozen:       true,
	}
}

func (c *Cluster) Reserve(id string, size uint64) {
	if _, has := c.Reservations[id]; !has {
		c.Reservations[id] = 0
	}

	c.Reservations[id] += size
	c.Used += size
}

func (c *Cluster) Commit(id string, size uint64) {
	if _, has := c.Reservations[id]; !has {
		return
	}

	c.Reservations[id] -= size
	c.Used -= c.Reservations[id]

	delete(c.Reservations, id)
}

func (c *Cluster) Discard(id string) {
	if _, has := c.Reservations[id]; !has {
		return
	}

	c.Used -= c.Reservations[id]

	delete(c.Reservations, id)
}

func (c *Cluster) Available() uint64 {
	return c.Size - c.Used
}

func (c *Cluster) Node(nodeId string) *Node {
	for _, n := range c.Nodes {
		if strings.Compare(n.Id, nodeId) == 0 {
			return n
		}
	}
	return nil
}

func (c *Cluster) Delete(nodeId string, masterChangedHandler func(*Node) error) error {
	for i, n := range c.Nodes {
		if strings.Compare(n.Id, nodeId) == 0 {
			c.Nodes = append(c.Nodes[:i], c.Nodes[i+1:]...)
			if n.Master && len(c.Nodes) > 0 {
				c.Nodes[0].Master = true
				return masterChangedHandler(c.Nodes[0])
			}
			return nil
		}
	}
	return errors.ErrNotFound
}

func (c *Cluster) SetMaster(nodeId string) error {
	for _, n := range c.Nodes {
		n.Master = strings.Compare(n.Id, nodeId) == 0
	}
	sort.Sort(c.Nodes)

	if c.Master() == nil {
		return errors.ErrNotFound
	}
	return nil
}

func (c *Cluster) Master() *Node {
	for _, n := range c.Nodes {
		if n.Master {
			return n
		}
	}
	return nil
}

func (c *Cluster) Slaves() NodeList {
	slaves := make(NodeList, 0)
	for _, n := range c.Nodes {
		if !n.Master {
			slaves = append(slaves, n)
		}
	}
	return slaves
}

func (c *Cluster) HighQualityNode() *Node {
	quality := int64(^uint(0) >> 1) // MaxIntNumber
	nodeIndex := -1
	for i, n := range c.Nodes {
		if n.Quality < quality {
			quality = n.Quality
			nodeIndex = i
		}
	}

	if nodeIndex > -1 {
		return c.Nodes[nodeIndex]
	}
	return nil
}

func (c *Cluster) Others(nodeId string) NodeList {
	found := false
	others := make(NodeList, 0)
	for _, n := range c.Nodes {
		if strings.Compare(n.Id, nodeId) != 0 {
			others = append(others, n)
		} else {
			found = true
		}
	}
	if !found {
		return nil
	}
	return others
}
