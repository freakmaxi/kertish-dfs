package common

import (
	"math"
	"sort"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/errors"
)

// Cluster struct is to hold cluster details in dfs farm
type Cluster struct {
	Id           string            `json:"clusterId"`
	Size         uint64            `json:"size"`
	Used         uint64            `json:"used"`
	Nodes        NodeList          `json:"nodes"`
	Reservations map[string]uint64 `json:"reservations"`
	Paralyzed    bool              `json:"paralyzed"` // If none of the cluster nodes are reachable or not have sync. content in slaves to be master
	Frozen       bool              `json:"frozen"`    // Available for Read but Not Create and Delete
	Snapshots    Snapshots         `json:"snapshots"`
}

// Clusters is the definition of the pointer array of Cluster struct
type Clusters []*Cluster

func (c Clusters) Len() int           { return len(c) }
func (c Clusters) Less(i, j int) bool { return c[i].Weight() < c[j].Weight() }
func (c Clusters) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// NewCluster initialises a new Cluster struct
func NewCluster(id string) *Cluster {
	return &Cluster{
		Id:           id,
		Nodes:        make(NodeList, 0),
		Reservations: make(map[string]uint64),
		Paralyzed:    true,
		Frozen:       true,
	}
}

// Reserve adds the reservation id to the cluster to ensure that
// simultaneous write request will have enough space in the cluster
func (c *Cluster) Reserve(id string, size uint64) {
	if _, has := c.Reservations[id]; !has {
		c.Reservations[id] = 0
	}

	c.Reservations[id] += size
	c.Used += size
}

// Commit commits the reservation and the used space in the cluster and
// drops the reservation from the cluster
func (c *Cluster) Commit(id string, size uint64) {
	if _, has := c.Reservations[id]; !has {
		return
	}

	c.Reservations[id] -= size
	c.Used -= c.Reservations[id]

	delete(c.Reservations, id)
}

// Discard discards the reservation from the cluster and free the reserved space
func (c *Cluster) Discard(id string) {
	if _, has := c.Reservations[id]; !has {
		return
	}

	c.Used -= c.Reservations[id]

	delete(c.Reservations, id)
}

// Available returns the available space in the cluster
func (c *Cluster) Available() uint64 {
	return c.Size - c.Used
}

// Weight calculates the load of the cluster as percent. MaxValue is 1 (theoretical)
func (c *Cluster) Weight() float64 {
	weight := float64(c.Used) / float64(c.Size) * 1000
	return math.Round(weight) / 1000
}

// Node searches the node in the cluster by its id
func (c *Cluster) Node(nodeId string) *Node {
	for _, n := range c.Nodes {
		if strings.Compare(n.Id, nodeId) == 0 {
			return n
		}
	}
	return nil
}

// Delete deletes the node from the cluster.
// masterChangedHandler executed when the deleted node is the
// current master node and there are other nodes in the cluster
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

// SetMaster changes the master in the cluster.
// If node is not exists, ErrNotFound will return
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

// Master returns the current master Node struct in the cluster
func (c *Cluster) Master() *Node {
	for _, n := range c.Nodes {
		if n.Master {
			return n
		}
	}
	return nil
}

// Slaves returns only the slave nodes other than the master node
func (c *Cluster) Slaves() NodeList {
	slaves := make(NodeList, 0)
	for _, n := range c.Nodes {
		if !n.Master {
			slaves = append(slaves, n)
		}
	}
	return slaves
}

// PrioritizedHighQualityNodes returns the most responsive Nodes in the cluster ordered by their quality
// if there is not any node with a good quality, it returns nil
func (c *Cluster) PrioritizedHighQualityNodes(nodeIdsMap CacheFileItemLocationMap) PrioritizedHighQualityNodeList {
	nodeList := make(PrioritizedHighQualityNodeList, 0)

	for _, n := range c.Nodes {
		if exists, has := nodeIdsMap[n.Id]; !has || !exists {
			continue
		}

		nodeList = append(nodeList, n)
	}

	if len(nodeList) > 0 {
		return nodeList
	}
	return nil
}

// Others returns the nodes in the cluster other than the one provided in the nodeId
// if there is not any node in the cluster other than the one provided in the nodeId, returns nil
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
