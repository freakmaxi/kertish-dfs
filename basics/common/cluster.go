package common

import (
	"math"
	"sort"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/errors"
)

const reservationDuration = time.Hour * 24 // 24 hours
const qualityDifferenceThreshold = 20      // 20 milliseconds

type States int

const (
	StateOnline   States = 0
	StateReadonly States = 1
	StateOffline  States = -1
)

type Topics string

const (
	TopicNone            Topics = ""
	TopicSynchronisation Topics = "Synchronisation"
	TopicBalance         Topics = "Balance"
	TopicMove            Topics = "Move"
	TopicRepair          Topics = "Repair"
	TopicUnregisterNode  Topics = "Unregister Node"
	TopicCreateSnapshot  Topics = "Create Snapshot"
	TopicDeleteSnapshot  Topics = "Delete Snapshot"
	TopicRestoreSnapshot Topics = "Restore Snapshot"
)

// Cluster struct is to hold cluster details in dfs farm
type Cluster struct {
	Id           string       `json:"clusterId"`
	Size         uint64       `json:"size"`
	Used         uint64       `json:"used"`
	Nodes        NodeList     `json:"nodes"`
	Reservations Reservations `json:"reservations"`

	// If master node is unreachable and also unable to elect a new master in the cluster
	Paralyzed bool `json:"paralyzed"`

	// 0 = Online, 1 = Readonly, -1 = Offline
	// Readonly mode: Create and Delete file operations are forbidden.
	// Offline mode:  All file operations for the cluster is forbidden
	State States `json:"state"`

	// Cluster can be in any state in maintain mode but all administrative
	// operations are forbidden when the cluster is in this mode
	Maintain      bool   `json:"maintain"`
	MaintainTopic Topics `json:"maintainTopic"`

	Snapshots Snapshots `json:"snapshots"`
}

// Reservations point the type declaration
type Reservations map[string]*Reservation

// NewReservations initialises a new Reservations struct
func NewReservations() Reservations {
	return make(Reservations)
}

func (r Reservations) CleanUp() {
	deletingReservationIds := make([]string, 0)

	for reservationId, reservation := range r {
		if reservation.ExpiresAt.Before(time.Now().UTC()) {
			deletingReservationIds = append(deletingReservationIds, reservationId)
		}
	}

	for _, reservationId := range deletingReservationIds {
		delete(r, reservationId)
	}
}

// Reservation struct is to hold the file creation reservation information specific to the cluster
type Reservation struct {
	Size      uint64    `json:"size"`
	ExpiresAt time.Time `json:"expiresAt"`
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
		Reservations: NewReservations(),
		Paralyzed:    true,
		State:        StateOffline,
		Maintain:     true,
	}
}

// Reserve adds the reservation id to the cluster to ensure that
// simultaneous write request will have enough space in the cluster
func (c *Cluster) Reserve(id string, size uint64) {
	if _, has := c.Reservations[id]; !has {
		c.Reservations[id] =
			&Reservation{
				Size:      0,
				ExpiresAt: time.Now().UTC().Add(reservationDuration),
			}
	}

	c.Reservations[id].Size += size
	c.Used += size
}

// Commit commits the reservation and the used space in the cluster and
// drops the reservation from the cluster
func (c *Cluster) Commit(id string, size uint64) {
	if _, has := c.Reservations[id]; !has {
		return
	}

	c.Reservations[id].Size -= size
	c.Used -= c.Reservations[id].Size

	c.Reservations[id].ExpiresAt = time.Now().UTC()
}

// Discard discards the reservation from the cluster and free the reserved space
func (c *Cluster) Discard(id string) {
	if _, has := c.Reservations[id]; !has {
		return
	}

	c.Used -= c.Reservations[id].Size

	c.Reservations[id].ExpiresAt = time.Now().UTC()
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
		if n.Master {
			n.SetLeadDuration()
		}
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
	sort.Sort(nodeList)

	if len(nodeList) > 0 {
		return nodeList
	}
	return nil
}

// HighQualityMasterNodeCandidate returns the most responsive Node that can be evaluated as master node
// if there is no candidate, returns nil
func (c *Cluster) HighQualityMasterNodeCandidate() *Node {
	masterNode := c.Master()
	if !masterNode.LeadershipExpired() {
		return nil
	}
	slaveNodes := c.Slaves()

	var bestDiffValue = int64(^uint64(0) >> 1)
	var bestNodeCandidate *Node

	for _, slaveNode := range slaveNodes {
		diff := masterNode.Quality - slaveNode.Quality

		if diff < 0 {
			continue
		}

		if diff > bestDiffValue {
			continue
		}

		if diff >= qualityDifferenceThreshold {
			bestDiffValue = diff
			bestNodeCandidate = slaveNode
		}
	}

	return bestNodeCandidate
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

func (c *Cluster) CanSchedule() bool {
	return c.State == StateOnline && !c.Paralyzed && !c.Maintain
}

// StateString returns the cluster current situation as string representative
func (c *Cluster) StateString() string {
	state := "Online"
	if c.State == StateReadonly {
		state = "Online (RO)"
	}

	switch c.State {
	case StateOnline, StateReadonly:
		if c.Paralyzed {
			return "Paralyzed"
		}
	case StateOffline:
		return "Offline"
	}

	return state
}
