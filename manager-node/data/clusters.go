package data

import (
	"context"
	"io"
	"sort"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/locking-center-client-go/mutex"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Clusters interface {
	RegisterCluster(cluster *common.Cluster) error
	UnRegisterCluster(clusterId string, clusterHandler func(cluster *common.Cluster) error) error

	RegisterNodeTo(clusterId string, node *common.Node) error
	UnRegisterNode(nodeId string, syncHandler func(cluster *common.Cluster) error, unregisteredNodeHandler func(deletingNode *common.Node) error, masterChangedHandler func(newMaster *common.Node) error) error

	Get(clusterId string) (*common.Cluster, error)
	GetAll() (common.Clusters, error)

	Save(clusterId string, saveHandler func(cluster *common.Cluster) error) error
	SaveAll(saveAllHandler func(clusters common.Clusters) error) error

	SetNewMaster(clusterId string, nodeId string) error
	UpdateNodes(cluster *common.Cluster) error
	ResetStats(cluster *common.Cluster) error
	SetFreeze(clusterId string, frozen bool) error

	ClusterIdOf(nodeId string) (string, error)
}

const clusterCollection = "cluster"
const clusterLockKey = "clusters"

type clusters struct {
	mutex mutex.LockingCenter

	conn *Connection
	col  *mongo.Collection
}

func NewClusters(conn *Connection, database string, mutex mutex.LockingCenter) (Clusters, error) {
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

func (c *clusters) context(parentContext context.Context) (context.Context, context.CancelFunc) {
	timeoutDuration := time.Second * 30
	return context.WithTimeout(parentContext, timeoutDuration)
}

func (c *clusters) next(cursor *mongo.Cursor) (*common.Cluster, error) {
	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	if !cursor.Next(ctx) {
		return nil, io.EOF
	}

	var cluster *common.Cluster
	if err := cursor.Decode(&cluster); err != nil {
		return nil, err
	}
	return cluster, nil
}

func (c *clusters) setupIndices() error {
	models := []mongo.IndexModel{
		{Keys: bson.M{"clusterId": 1}},
		{Keys: bson.M{"nodes.id": 1}},
	}

	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	_, err := c.col.Indexes().CreateMany(ctx, models)
	return err
}

func (c *clusters) RegisterCluster(cluster *common.Cluster) error {
	c.mutex.Lock(clusterLockKey)
	defer c.mutex.Unlock(clusterLockKey)

	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	if err := c.col.FindOne(ctx, bson.M{"clusterId": cluster.Id}).Err(); err == nil {
		return errors.ErrExists
	} else {
		if err != mongo.ErrNoDocuments {
			return err
		}
	}
	return c.overwrite(common.Clusters{cluster})
}

func (c *clusters) UnRegisterCluster(clusterId string, clusterHandler func(cluster *common.Cluster) error) error {
	cluster, err := c.Get(clusterId)
	if err != nil {
		return err
	}
	if err := clusterHandler(cluster); err != nil {
		return err
	}

	c.mutex.Lock(clusterLockKey)
	defer c.mutex.Unlock(clusterLockKey)

	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	if _, err := c.col.DeleteOne(ctx, bson.M{"clusterId": clusterId}); err != nil {
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

func (c *clusters) UnRegisterNode(nodeId string, syncHandler func(cluster *common.Cluster) error, unregisteredNodeHandler func(deletingNode *common.Node) error, masterChangedHandler func(newMaster *common.Node) error) error {
	nodeCluster, err := c.getClusterByNodeId(nodeId)
	if err != nil {
		return err
	}
	deletingNode := nodeCluster.Node(nodeId)

	if deletingNode.Master {
		if err := syncHandler(nodeCluster); err != nil {
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

func (c *clusters) Get(clusterId string) (*common.Cluster, error) {
	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	var cluster *common.Cluster

	if err := c.col.FindOne(ctx, bson.M{"clusterId": clusterId}).Decode(&cluster); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errors.ErrNotFound
		}
		return nil, err
	}

	return cluster, nil
}

func (c *clusters) GetAll() (common.Clusters, error) {
	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	cursor, err := c.col.Find(ctx, bson.M{})
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, errors.ErrNotFound
		}
		return nil, err
	}
	defer func() {
		ctx, cancelFunc := c.context(context.Background())
		defer cancelFunc()

		_ = cursor.Close(ctx)
	}()

	clusters := make(common.Clusters, 0)
	for {
		cluster, err := c.next(cursor)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		clusters = append(clusters, cluster)
	}
	sort.Sort(clusters)

	return clusters, nil
}

func (c *clusters) Save(clusterId string, saveHandler func(cluster *common.Cluster) error) error {
	c.mutex.Lock(clusterLockKey)
	defer c.mutex.Unlock(clusterLockKey)

	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	var cluster *common.Cluster
	filter := bson.M{"clusterId": clusterId}

	if err := c.col.FindOne(ctx, filter).Decode(&cluster); err != nil {
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
	c.mutex.Lock(clusterLockKey)
	defer c.mutex.Unlock(clusterLockKey)

	getClustersFunc := func() (common.Clusters, error) {
		ctx, cancelFunc := c.context(context.Background())
		defer cancelFunc()

		cursor, err := c.col.Find(ctx, bson.M{})
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, errors.ErrNotFound
			}
			return nil, err
		}
		defer func() {
			ctx, cancelFunc := c.context(context.Background())
			defer cancelFunc()

			_ = cursor.Close(ctx)
		}()

		clusters := make(common.Clusters, 0)
		for {
			cluster, err := c.next(cursor)
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
			clusters = append(clusters, cluster)
		}
		return clusters, nil
	}

	clusters, err := getClustersFunc()
	if err != nil {
		return err
	}

	if err := saveAllHandler(clusters); err != nil {
		return err
	}
	return c.overwrite(clusters)
}

func (c *clusters) SetNewMaster(clusterId string, masterNodeId string) error {
	return c.Save(clusterId, func(cluster *common.Cluster) error {
		return cluster.SetMaster(masterNodeId)
	})
}

func (c *clusters) UpdateNodes(cluster *common.Cluster) error {
	c.mutex.Lock(clusterLockKey)
	defer c.mutex.Unlock(clusterLockKey)

	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	filter := bson.M{
		"clusterId": cluster.Id,
		"frozen":    false,
	}
	update := bson.M{
		"$set": bson.M{
			"nodes":     cluster.Nodes,
			"paralyzed": cluster.Paralyzed,
		},
	}

	_, err := c.col.UpdateOne(ctx, filter, update)
	return err
}

func (c *clusters) ResetStats(cluster *common.Cluster) error {
	c.mutex.Lock(clusterLockKey)
	defer c.mutex.Unlock(clusterLockKey)

	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	filter := bson.M{"clusterId": cluster.Id}
	update := bson.M{
		"$set": bson.M{
			"reservations": cluster.Reservations,
			"used":         cluster.Used,
			"snapshots":    cluster.Snapshots,
		},
	}

	_, err := c.col.UpdateOne(ctx, filter, update)
	return err
}

func (c *clusters) SetFreeze(clusterId string, frozen bool) error {
	c.mutex.Lock(clusterLockKey)
	defer c.mutex.Unlock(clusterLockKey)

	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	filter := bson.M{
		"clusterId": clusterId,
		"frozen":    !frozen,
	}
	update := bson.M{
		"$set": bson.M{
			"paralyzed": true,
			"frozen":    frozen,
		},
	}

	if _, err := c.col.UpdateOne(ctx, filter, update); err != nil && err != mongo.ErrNoDocuments {
		return err
	}
	return nil
}

func (c *clusters) ClusterIdOf(nodeId string) (string, error) {
	cluster, err := c.getClusterByNodeId(nodeId)
	if err != nil {
		return "", err
	}
	return cluster.Id, nil
}

func (c *clusters) getClusterByNodeId(nodeId string) (*common.Cluster, error) {
	ctx, cancelFunc := c.context(context.Background())
	defer cancelFunc()

	var cluster *common.Cluster
	if err := c.col.FindOne(ctx, bson.M{"nodes.id": nodeId}).Decode(&cluster); err != nil {
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

	updateOneFunc := func(parentContext context.Context, cluster *common.Cluster) error {
		ctx, cancelFunc := c.context(parentContext)
		defer cancelFunc()

		opts := (&options.UpdateOptions{}).SetUpsert(true)
		_, err := c.col.UpdateOne(ctx, bson.M{"clusterId": cluster.Id}, bson.M{"$set": cluster}, opts)
		return err
	}

	ctxS1, cancelS1Func := c.context(context.Background())
	defer cancelS1Func()

	if err = mongo.WithSession(ctxS1, session, func(sc mongo.SessionContext) error {
		if err = sc.StartTransaction(); err != nil {
			return err
		}

		var parentContext context.Context = sc
		if !c.conn.transaction {
			parentContext = context.Background()
		}

		for _, cluster := range clusters {
			sort.Sort(cluster.Nodes)
			sort.Sort(cluster.Snapshots)

			if err := updateOneFunc(parentContext, cluster); err != nil {
				return err
			}
		}

		return sc.CommitTransaction(parentContext)
	}); err != nil {
		return err
	}

	ctxS2, cancelS2Func := c.context(context.Background())
	defer cancelS2Func()

	session.EndSession(ctxS2)

	return nil
}

var _ Clusters = &clusters{}
