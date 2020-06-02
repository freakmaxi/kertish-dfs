package data

import (
	"context"
	"os"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/locking-center-client-go/mutex"
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
const metadataLockKey = "metadata"

type metadata struct {
	mutex mutex.LockingCenter
	conn  *Connection
	col   *mongo.Collection
}

func NewMetadata(mutex mutex.LockingCenter, conn *Connection, database string) (Metadata, error) {
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
	m.mutex.Wait(metadataLockKey)

	total, err := m.col.CountDocuments(m.context(), bson.M{})
	if err != nil {
		return err
	}

	opts := options.Find()
	opts.SetSort(bson.M{"full": -1})
	opts.SetProjection(bson.M{"_id": 1, "full": 1})
	opts.SetNoCursorTimeout(true)

	cursor, err := m.col.Find(m.context(), bson.M{}, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return os.ErrNotExist
		}
		return err
	}
	defer cursor.Close(m.context())

	handlerFunc := func(id primitive.ObjectID, folderPath string) error {
		m.mutex.Lock(folderPath)
		defer m.mutex.Unlock(folderPath)

		var folder *common.Folder
		if err := m.col.FindOne(m.context(), bson.M{"_id": id}).Decode(&folder); err != nil {
			if err == mongo.ErrNoDocuments {
				return nil
			}
			return err
		}

		changed, err := folderHandler(folder)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}

		return m.save([]*common.Folder{folder}, false)
	}

	handled := int64(0)
	for cursor.Next(m.context()) {
		id := cursor.Current.Lookup("_id").ObjectID()
		folderPath := cursor.Current.Lookup("full").StringValue()

		if err := handlerFunc(id, folderPath); err != nil {
			return err
		}
		handled++
	}
	if handled != total {
		return os.ErrInvalid
	}

	return nil
}

func (m *metadata) LockTree(folderHandler func(folders []*common.Folder) ([]*common.Folder, error)) error {
	m.mutex.Lock(metadataLockKey)
	defer m.mutex.Unlock(metadataLockKey)

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
	defer cursor.Close(m.context())

	folders := make([]*common.Folder, 0)
	for cursor.Next(m.context()) {
		var folder *common.Folder
		if err := cursor.Decode(&folder); err != nil {
			return err
		}
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

	if err = mongo.WithSession(m.context(), session, func(sc mongo.SessionContext) error {
		if err = sc.StartTransaction(); err != nil {
			return err
		}

		for _, folder := range folders {
			filter := bson.M{"full": folder.Full}

			opts := (&options.UpdateOptions{}).SetUpsert(upsert)
			if _, err := m.col.UpdateOne(m.context(), filter, bson.M{"$set": folder}, opts); err != nil {
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
