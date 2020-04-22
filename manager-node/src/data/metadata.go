package data

import (
	"context"
	"os"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Metadata interface {
	Cursor(folderHandler func(folder *common.Folder) (bool, error)) error
	LockTree(folderHandler func(folders []*common.Folder) ([]*common.Folder, error)) error
}

const metadataCollection = "metadata"

type metadata struct {
	mutex Mutex
	conn  *Connection
	col   *mongo.Collection
}

func NewMetadata(mutex Mutex, conn *Connection, database string) (Metadata, error) {
	dfsCol := conn.client.Database(database).Collection(metadataCollection)

	return &metadata{
		mutex: mutex,
		conn:  conn,
		col:   dfsCol,
	}, nil
}

func (m *metadata) context() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	return ctx
}

func (m *metadata) Cursor(folderHandler func(folder *common.Folder) (bool, error)) error {
	opts := options.Find()
	opts.SetSort(bson.M{"full": -1})

	cursor, err := m.col.Find(m.context(), bson.M{}, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return os.ErrNotExist
		}
		return err
	}
	defer cursor.Close(m.context())

	for cursor.Next(m.context()) {
		var folder *common.Folder
		if err := cursor.Decode(&folder); err != nil {
			return err
		}

		m.mutex.Lock(folder.Full)
		changed, err := folderHandler(folder)
		if err != nil {
			m.mutex.UnLock(folder.Full)
			return err
		}
		if !changed {
			m.mutex.UnLock(folder.Full)
			continue
		}
		if err := m.save([]*common.Folder{folder}, false); err != nil {
			m.mutex.UnLock(folder.Full)
			return err
		}
		m.mutex.UnLock(folder.Full)
	}

	return nil
}

func (m *metadata) LockTree(folderHandler func(folders []*common.Folder) ([]*common.Folder, error)) error {
	opts := options.Find()
	opts.SetSort(bson.M{"full": 1})

	filter := bson.M{"full": bson.M{"$regex": primitive.Regex{Pattern: "^/.*"}}}

	cursor, err := m.col.Find(m.context(), filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return os.ErrNotExist
		}
		return err
	}

	folders := make([]*common.Folder, 0)
	defer func() {
		for _, folder := range folders {
			m.mutex.UnLock(folder.Full)
		}
	}()
	for cursor.Next(m.context()) {
		var folder *common.Folder
		if err := cursor.Decode(&folder); err != nil {
			return err
		}
		m.mutex.Lock(folder.Full)
		folders = append(folders, folder)
	}

	result, err := folderHandler(folders)
	if err != nil {
		return err
	}

	if result == nil {
		return nil
	}

	return m.save(result, true)
}

func (m *metadata) save(folders []*common.Folder, upsert bool) error {
	session, err := m.conn.client.StartSession()
	if err != nil {
		return err
	}
	if err = session.StartTransaction(); err != nil {
		return err
	}

	if err = mongo.WithSession(m.context(), session, func(sc mongo.SessionContext) error {
		for _, folder := range folders {
			filter := bson.M{"full": folder.Full}

			opts := (&options.UpdateOptions{}).SetUpsert(upsert)
			if _, err := m.col.UpdateOne(m.context(), filter, bson.D{{"$set", folder}}, opts); err != nil {
				return err
			}
		}

		return sc.CommitTransaction(m.context())
	}); err != nil {
		return err
	}

	session.EndSession(m.context())

	return nil
}

var _ Metadata = &metadata{}
