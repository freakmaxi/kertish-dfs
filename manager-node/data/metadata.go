package data

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/locking-center-client-go/mutex"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Metadata interface {
	Cursor(folderHandler func(folder *common.Folder) (bool, error), parallelSize uint8) error
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

func (m *metadata) context(sc context.Context) context.Context {
	if sc != nil && m.conn.transaction {
		return sc
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	return ctx
}

func (m *metadata) Cursor(folderHandler func(folder *common.Folder) (bool, error), parallelSize uint8) error {
	m.mutex.Wait(metadataLockKey)

	semaphoreChan := make(chan bool, parallelSize)
	for i := 0; i < cap(semaphoreChan); i++ {
		semaphoreChan <- true
	}
	defer close(semaphoreChan)

	total, err := m.col.CountDocuments(m.context(nil), bson.M{})
	if err != nil {
		return err
	}

	opts := options.Find()
	opts.SetSort(bson.M{"full": -1})
	opts.SetProjection(bson.M{"_id": 1, "full": 1})
	opts.SetNoCursorTimeout(true)

	cursor, err := m.col.Find(m.context(nil), bson.M{}, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return os.ErrNotExist
		}
		return err
	}
	defer func() { _ = cursor.Close(m.context(nil)) }()

	handlerFunc := func(wg *sync.WaitGroup, id primitive.ObjectID, folderPath string, errorChan chan error) {
		defer wg.Done()
		defer func() { semaphoreChan <- true }()

		m.mutex.Lock(folderPath)
		defer m.mutex.Unlock(folderPath)

		var folder *common.Folder
		if err := m.col.FindOne(m.context(nil), bson.M{"_id": id}).Decode(&folder); err != nil {
			if err == mongo.ErrNoDocuments {
				return
			}
			errorChan <- err
			return
		}

		changed, err := folderHandler(folder)
		if err != nil {
			errorChan <- err
			return
		}
		if !changed {
			return
		}

		if err := m.save([]*common.Folder{folder}, false); err != nil {
			errorChan <- err
		}
	}

	handled := int64(0)
	wg := &sync.WaitGroup{}
	errorChan := make(chan error, parallelSize)

	for cursor.Next(m.context(nil)) {
		id := cursor.Current.Lookup("_id").ObjectID()
		folderPath := cursor.Current.Lookup("full").StringValue()

		wg.Add(1)
		go handlerFunc(wg, id, folderPath, errorChan)

		handled++

		<-semaphoreChan

		if len(errorChan) > 0 {
			break
		}
	}
	wg.Wait()

	close(errorChan)

	bulkError := errors.NewBulkError()

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			select {
			case err, more := <-errorChan:
				if !more {
					return
				}
				bulkError.Add(err)
			}
		}
	}(wg)
	wg.Wait()

	if bulkError.HasError() {
		return bulkError
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

	cursor, err := m.col.Find(m.context(nil), filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return os.ErrNotExist
		}
		return err
	}
	defer func() { _ = cursor.Close(m.context(nil)) }()

	folders := make([]*common.Folder, 0)
	for cursor.Next(m.context(nil)) {
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

	if err = mongo.WithSession(m.context(nil), session, func(sc mongo.SessionContext) error {
		if err = sc.StartTransaction(); err != nil {
			return err
		}

		for _, folder := range folders {
			filter := bson.M{"full": folder.Full}

			opts := (&options.UpdateOptions{}).SetUpsert(upsert)
			if _, err := m.col.UpdateOne(m.context(sc), filter, bson.M{"$set": folder}, opts); err != nil {
				return err
			}
		}

		return sc.CommitTransaction(m.context(sc))
	}); err != nil {
		return err
	}

	session.EndSession(m.context(nil))

	return nil
}

var _ Metadata = &metadata{}
