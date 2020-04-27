package data

import (
	"context"
	"sort"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Clusters interface {
	RegisterCluster(cluster *common.Cluster) error
	UnRegisterCluster(clusterId string, clusterHandler func(cluster *common.Cluster) error) error

	RegisterNodeTo(clusterId string, node *common.Node) error
	UnRegisterNode(nodeId string, syncHandler func(clusterId string) error, unregisteredNodeHandler func(deletingNode *common.Node) error, masterChangedHandler func(newMaster *common.Node) error) error

	Lock(clusterId string, clusterHandler func(cluster *common.Cluster) error) error
	LockAll(clusterHandler func(clusters common.Clusters) error) error

	Save(clusterId string, saveHandler func(cluster *common.Cluster) error) error
	SaveAll(saveAllHandler func(clusters common.Clusters) error) error

	SetNewMaster(clusterId string, nodeId string) error
	UpdateNodes(cluster *common.Cluster) error

	ClusterIdOf(nodeId string) (*string, error)
}

const clusterCollection = "cluster"
const clustersLockKey = "clusters"

type clusters struct {
	mutex Mutex
	conn  *Connection
	col   *mongo.Collection
}

func NewClusters(mutex Mutex, conn *Connection, database string) (Clusters, error) {
	dfsCol := conn.client.Database(database).Collection(clusterCollection)

	c := &clusters{
		mutex: mutex,
		conn:  conn,
		col:   dfsCol,
	}
	if err := c.setupIndices(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *clusters) context() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	return ctx
}

func (c *clusters) setupIndices() error {
	models := []mongo.IndexModel{
		{Keys: bson.M{"clusterId": 1}},
		{Keys: bson.M{"nodes.id": 1}},
	}
	_, err := c.col.Indexes().CreateMany(c.context(), models, nil)
	return err
}

func (c *clusters) RegisterCluster(cluster *common.Cluster) error {
	c.mutex.Lock(clustersLockKey)
	defer c.mutex.UnLock(clustersLockKey)

	c.mutex.Lock(cluster.Id)
	defer c.mutex.UnLock(cluster.Id)

	filter := bson.M{"clusterId": cluster.Id}

	if err := c.col.FindOne(c.context(), filter).Err(); err == nil {
		return errors.ErrExists
	} else {
		if err != mongo.ErrNoDocuments {
			return err
		}
	}
	return c.overwrite(common.Clusters{cluster})
}

func (c *clusters) UnRegisterCluster(clusterId string, clusterHandler func(cluster *common.Cluster) error) error {
	if err := c.Lock(clusterId, clusterHandler); err != nil {
		return err
	}

	c.mutex.Lock(clusterId)
	defer c.mutex.UnLock(clusterId)

	if _, err := c.col.DeleteOne(c.context(), bson.M{"clusterId": clusterId}); err != nil {
		if err == mongo.ErrNoDocuments {
			return errors.ErrNotFound
		}
		return err
	}
	return nil
}

func (c *clusters) RegisterNodeTo(clusterId string, node *common.Node) error {
	_, err := c.getClusterByNodeId(node.Id)
	if err == nil {
		return errors.ErrRegistered
	} else {
		if err != errors.ErrNotFound {
			return err
		}
	}

	return c.Save(clusterId, func(cluster *common.Cluster) error {
		examNode := cluster.Node(node.Id)
		if examNode != nil {
			return errors.ErrRegistered
		}
		cluster.Nodes = append(cluster.Nodes, node)
		return nil
	})
}

func (c *clusters) UnRegisterNode(nodeId string, syncHandler func(clusterId string) error, unregisteredNodeHandler func(deletingNode *common.Node) error, masterChangedHandler func(newMaster *common.Node) error) error {
	nodeCluster, err := c.getClusterByNodeId(nodeId)
	if err != nil {
		return err
	}
	deletingNode := nodeCluster.Node(nodeId)

	if deletingNode.Master {
		if err := syncHandler(nodeCluster.Id); err != nil {
			return err
		}
	}

	return c.Save(nodeCluster.Id, func(cluster *common.Cluster) error {
		others := cluster.Others(nodeId)
		if len(others) == 0 {
			return errors.ErrLastNode
		}

		if err := unregisteredNodeHandler(deletingNode); err != nil {
			return err
		}

		if err := cluster.Delete(nodeId, func(newMaster *common.Node) error {
			return masterChangedHandler(newMaster)
		}); err != nil {
			return err
		}
		return nil
	})
}

func (c *clusters) Lock(clusterId string, clusterHandler func(cluster *common.Cluster) error) error {
	c.mutex.Wait(clustersLockKey)
	c.mutex.Wait(clusterId)

	var cluster *common.Cluster
	filter := bson.M{"clusterId": clusterId}

	if err := c.col.FindOne(c.context(), filter).Decode(&cluster); err != nil {
		if err == mongo.ErrNoDocuments {
			return errors.ErrNotFound
		}
		return err
	}

	if err := clusterHandler(cluster); err != nil {
		return err
	}
	return nil
}

func (c *clusters) LockAll(clusterHandler func(clusters common.Clusters) error) error {
	c.mutex.Wait(clustersLockKey)

	cur, err := c.col.Find(c.context(), bson.M{})
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return errors.ErrNotFound
		}
		return err
	}
	defer cur.Close(c.context())

	clusters := make(common.Clusters, 0)
	for cur.Next(c.context()) {
		var cluster *common.Cluster
		if err := cur.Decode(&cluster); err != nil {
			return err
		}
		clusters = append(clusters, cluster)
		c.mutex.Wait(cluster.Id)
	}

	sort.Sort(clusters)
	if err := clusterHandler(clusters); err != nil {
		return err
	}
	return nil
}

func (c *clusters) Save(clusterId string, saveHandler func(cluster *common.Cluster) error) error {
	c.mutex.Lock(clustersLockKey)
	defer c.mutex.UnLock(clustersLockKey)

	c.mutex.Lock(clusterId)
	defer c.mutex.UnLock(clusterId)

	var cluster *common.Cluster
	filter := bson.M{"clusterId": clusterId}

	if err := c.col.FindOne(c.context(), filter).Decode(&cluster); err != nil {
		if err == mongo.ErrNoDocuments {
			return errors.ErrNotFound
		}
		return err
	}

	if err := saveHandler(cluster); err != nil {
		return err
	}
	return c.overwrite(common.Clusters{cluster})
}

func (c *clusters) SaveAll(saveAllHandler func(clusters common.Clusters) error) error {
	c.mutex.Lock(clustersLockKey)
	defer c.mutex.UnLock(clustersLockKey)

	cur, err := c.col.Find(c.context(), bson.M{})
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return errors.ErrNotFound
		}
		return err
	}
	defer cur.Close(c.context())

	clusterIds := make([]string, 0)
	clusters := make(common.Clusters, 0)
	for cur.Next(c.context()) {
		var cluster *common.Cluster
		if err := cur.Decode(&cluster); err != nil {
			return err
		}

		clusters = append(clusters, cluster)
		clusterIds = append(clusterIds, cluster.Id)
		c.mutex.Lock(cluster.Id)
	}
	defer func() {
		for _, clusterId := range clusterIds {
			c.mutex.UnLock(clusterId)
		}
	}()

	sort.Sort(clusters)
	if err := saveAllHandler(clusters); err != nil {
		return err
	}
	return c.overwrite(clusters)
}

func (c *clusters) SetNewMaster(clusterId string, masterNodeId string) error {
	return c.Save(clusterId, func(cluster *common.Cluster) error {
		if err := cluster.SetMaster(masterNodeId); err != nil {
			return err
		}
		return nil
	})
}

func (c *clusters) UpdateNodes(cluster *common.Cluster) error {
	c.mutex.Wait(cluster.Id)

	filter := bson.M{"clusterId": cluster.Id}
	update := bson.M{"$set": bson.M{"nodes": cluster.Nodes}}

	_, err := c.col.UpdateOne(c.context(), filter, update)
	return err
}

func (c *clusters) ClusterIdOf(nodeId string) (*string, error) {
	cluster, err := c.getClusterByNodeId(nodeId)
	if err != nil {
		return nil, err
	}
	return &cluster.Id, nil
}

func (c *clusters) getClusterByNodeId(nodeId string) (*common.Cluster, error) {
	var cluster *common.Cluster
	filter := bson.M{"nodes.id": nodeId}

	if err := c.col.FindOne(c.context(), filter).Decode(&cluster); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errors.ErrNotFound
		}
		return nil, err
	}
	return cluster, nil
}

func (c *clusters) overwrite(clusters common.Clusters) error {
	session, err := c.conn.client.StartSession()
	if err != nil {
		return err
	}
	if err = session.StartTransaction(); err != nil {
		return err
	}

	if err = mongo.WithSession(c.context(), session, func(sc mongo.SessionContext) error {
		for _, cluster := range clusters {
			sort.Sort(cluster.Nodes)

			filter := bson.M{"clusterId": cluster.Id}
			opts := (&options.UpdateOptions{}).SetUpsert(true)
			if _, err := c.col.UpdateOne(c.context(), filter, bson.D{{"$set", cluster}}, opts); err != nil {
				return err
			}
		}
		return session.CommitTransaction(sc)
	}); err != nil {
		return err
	}
	session.EndSession(c.context())

	return nil
}

var _ Clusters = &clusters{}
