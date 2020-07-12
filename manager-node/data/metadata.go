package data

import (
	"context"
	"fmt"
	"io"
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

	Lock()
	Unlock()
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

func (m *metadata) context(parentContext context.Context) (context.Context, context.CancelFunc) {
	timeoutDuration := time.Second * 30
	return context.WithTimeout(parentContext, timeoutDuration)
}

func (m *metadata) countDocuments() (int64, error) {
	ctx, cancelFunc := m.context(context.Background())
	defer cancelFunc()

	return m.col.CountDocuments(ctx, bson.M{})
}

func (m *metadata) find(filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
	ctx, cancelFunc := m.context(context.Background())
	defer cancelFunc()

	return m.col.Find(ctx, filter, opts...)
}

func (m *metadata) findOne(filter interface{}, opts ...*options.FindOneOptions) (*common.Folder, error) {
	ctx, cancelFunc := m.context(context.Background())
	defer cancelFunc()

	var folder *common.Folder
	if err := m.col.FindOne(ctx, filter, opts...).Decode(&folder); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return folder, nil
}

func (m *metadata) next(cursor *mongo.Cursor) (*common.Folder, error) {
	ctx, cancelFunc := m.context(context.Background())
	defer cancelFunc()

	if !cursor.Next(ctx) {
		return nil, io.EOF
	}

	var folder *common.Folder
	if err := cursor.Decode(&folder); err != nil {
		return nil, err
	}
	return folder, nil
}

func (m *metadata) nextRaw(cursor *mongo.Cursor) (bson.Raw, error) {
	ctx, cancelFunc := m.context(context.Background())
	defer cancelFunc()

	if !cursor.Next(ctx) {
		return nil, io.EOF
	}
	return cursor.Current, nil
}

func (m *metadata) updateOne(parentContext context.Context, folder common.Folder) error {
	ctx, cancelFunc := m.context(parentContext)
	defer cancelFunc()

	opts := (&options.UpdateOptions{}).SetUpsert(true)
	_, err := m.col.UpdateOne(ctx, bson.M{"full": folder.Full}, bson.M{"$set": folder}, opts)
	return err
}

func (m *metadata) Lock() {
	m.mutex.Lock(metadataLockKey)
}

func (m *metadata) Unlock() {
	m.mutex.Unlock(metadataLockKey)
}

func (m *metadata) Cursor(folderHandler func(folder *common.Folder) (bool, error), parallelSize uint8) error {
	semaphoreChan := make(chan bool, parallelSize)
	for i := 0; i < cap(semaphoreChan); i++ {
		semaphoreChan <- true
	}
	defer close(semaphoreChan)

	total, err := m.countDocuments()
	if err != nil {
		return err
	}

	opts := options.Find()
	opts.SetSort(bson.M{"full": -1})
	opts.SetProjection(bson.M{"_id": 1, "full": 1})
	opts.SetNoCursorTimeout(true)

	cursor, err := m.find(bson.M{}, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return os.ErrNotExist
		}
		return err
	}
	defer func() {
		ctx, cancelFunc := m.context(context.Background())
		defer cancelFunc()

		_ = cursor.Close(ctx)
	}()

	handlerFunc := func(wg *sync.WaitGroup, id primitive.ObjectID, folderPath string, errorChan chan error) {
		defer wg.Done()
		defer func() { semaphoreChan <- true }()

		m.mutex.Lock(folderPath)
		defer m.mutex.Unlock(folderPath)

		folder, err := m.findOne(bson.M{"_id": id})
		if err != nil {
			if err == os.ErrNotExist {
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

	for {
		raw, err := m.nextRaw(cursor)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		id := raw.Lookup("_id").ObjectID()
		folderPath := raw.Lookup("full").StringValue()

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
		for err = range errorChan {
			bulkError.Add(err)
		}
	}(wg)
	wg.Wait()

	if bulkError.HasError() {
		return bulkError
	}

	if handled != total {
		return fmt.Errorf("total (%d) and handled (%d) document counts didn't match", total, handled)
	}

	return nil
}

func (m *metadata) LockTree(folderHandler func(folders []*common.Folder) ([]*common.Folder, error)) error {
	m.mutex.Lock(metadataLockKey)
	defer m.mutex.Unlock(metadataLockKey)

	opts := options.Find()
	opts.SetSort(bson.M{"full": 1})

	filter := bson.M{"full": bson.M{"$regex": primitive.Regex{Pattern: "^/.*"}}}

	cursor, err := m.find(filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return os.ErrNotExist
		}
		return err
	}
	defer func() {
		ctx, cancelFunc := m.context(context.Background())
		defer cancelFunc()

		_ = cursor.Close(ctx)
	}()

	folders := make([]*common.Folder, 0)
	for {
		folder, err := m.next(cursor)
		if err != nil {
			if err == io.EOF {
				break
			}
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

	ctxS1, cancelS1Func := m.context(context.Background())
	defer cancelS1Func()

	if err = mongo.WithSession(ctxS1, session, func(sc mongo.SessionContext) error {
		if err = sc.StartTransaction(); err != nil {
			return err
		}

		var parentContext context.Context = sc
		if !m.conn.transaction {
			parentContext = context.Background()
		}

		for _, folder := range folders {
			if err := m.updateOne(parentContext, *folder); err != nil {
				return err
			}
		}

		return sc.CommitTransaction(parentContext)
	}); err != nil {
		return err
	}

	ctxS2, cancelS2Func := m.context(context.Background())
	defer cancelS2Func()

	session.EndSession(ctxS2)

	return nil
}

var _ Metadata = &metadata{}
