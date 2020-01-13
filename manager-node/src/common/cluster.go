package common

import (
	"sort"
	"strings"

	"github.com/freakmaxi/2020-dfs/manager-node/src/errors"
)

type Cluster struct {
	Id           string         `json:"clusterId"`
	Size         uint64         `json:"size"`
	Used         uint64         `json:"used"`
	Nodes        NodeList       `json:"nodes"`
	Reservations []*Reservation `json:"reservations"`
}

type Clusters []*Cluster

func (c Clusters) Len() int           { return len(c) }
func (c Clusters) Less(i, j int) bool { return c[i].Used < c[j].Used }
func (c Clusters) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func NewCluster(id string) *Cluster {
	return &Cluster{
		Id:           id,
		Nodes:        NodeList{},
		Reservations: []*Reservation{},
	}
}

func (c *Cluster) Reserve(id string, size uint64) {
	for _, reservation := range c.Reservations {
		if strings.Compare(reservation.Id, id) == 0 {
			c.Used += size
			reservation.Size += size
			return
		}
	}
	c.Used += size
	c.Reservations = append(c.Reservations, &Reservation{Id: id, Size: size})
}

func (c *Cluster) Commit(id string, size uint64) {
	for i, reservation := range c.Reservations {
		if strings.Compare(reservation.Id, id) == 0 {
			reservation.Size -= size
			c.Used -= reservation.Size
			c.Reservations = append(c.Reservations[:i], c.Reservations[i+1:]...)
			return
		}
	}
}

func (c *Cluster) Discard(id string) {
	for i, reservation := range c.Reservations {
		if strings.Compare(reservation.Id, id) == 0 {
			c.Used -= reservation.Size
			c.Reservations = append(c.Reservations[:i], c.Reservations[i+1:]...)
			return
		}
	}
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
