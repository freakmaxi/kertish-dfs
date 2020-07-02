package data

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/locking-center-client-go/mutex"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LockReleaseHandler func()

type Metadata interface {
	Get(folderPaths []string) ([]*common.Folder, error)
	Tree(folderPath string, includeItself bool, reverseSort bool) ([]*common.Folder, error)

	SaveBlock(folderPaths []string, saveHandler func(folders map[string]*common.Folder) (bool, error)) error
	SaveChain(folderPath string, saveHandler func(folder *common.Folder) (bool, error)) error
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

	m := &metadata{
		mutex: mutex,
		conn:  conn,
		col:   dfsCol,
	}
	if err := m.setupIndices(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *metadata) context(parentContext context.Context) (context.Context, context.CancelFunc) {
	timeoutDuration := time.Second * 30
	return context.WithTimeout(parentContext, timeoutDuration)
}

func (m *metadata) setupIndices() error {
	model := mongo.IndexModel{
		Keys: bson.M{"full": 1},
	}

	ctx, cancelFunc := m.context(context.Background())
	defer cancelFunc()

	_, err := m.col.Indexes().CreateOne(ctx, model)
	return err
}

func (m *metadata) find(filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error) {
	ctx, cancelFunc := m.context(context.Background())
	defer cancelFunc()

	return m.col.Find(ctx, filter, opts...)
}

func (m *metadata) findOne(parentContext context.Context, filter interface{}, opts ...*options.FindOneOptions) (*common.Folder, error) {
	ctx, cancelFunc := m.context(parentContext)
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

func (m *metadata) updateOne(parentContext context.Context, folderPath string, folder common.Folder) error {
	ctx, cancelFunc := m.context(parentContext)
	defer cancelFunc()

	opts := (&options.UpdateOptions{}).SetUpsert(true)
	_, err := m.col.UpdateOne(ctx, bson.M{"full": folderPath}, bson.M{"$set": folder}, opts)
	return err
}

func (m *metadata) Get(folderPaths []string) ([]*common.Folder, error) {
	folderPaths = m.cleanDuplicates(folderPaths)

	findOneFunc := func(folderPath string) (*common.Folder, error) {
		ctx, cancelFunc := m.context(context.Background())
		defer cancelFunc()

		var folder *common.Folder
		if err := m.col.FindOne(ctx, bson.M{"full": folderPath}).Decode(&folder); err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, os.ErrNotExist
			}
			return nil, err
		}
		return folder, nil
	}

	folders := make([]*common.Folder, 0)
	for _, folderPath := range folderPaths {
		folder, err := findOneFunc(folderPath)
		if err != nil {
			return nil, err
		}
		folders = append(folders, folder)
	}
	return folders, nil
}

func (m *metadata) Tree(folderPath string, includeItself bool, reverseSort bool) ([]*common.Folder, error) {
	filterContent := []interface{}{
		bson.M{"full": bson.M{"$regex": primitive.Regex{Pattern: fmt.Sprintf("^%s/.+", folderPath)}}},
	}
	if includeItself {
		filterContent = append(filterContent, bson.M{"full": bson.M{"$regex": primitive.Regex{Pattern: fmt.Sprintf("^%s$", folderPath)}}})
	}
	filter := bson.M{"$or": filterContent}

	opts := options.Find()
	if !reverseSort {
		opts.SetSort(bson.M{"full": 1})
	} else {
		opts.SetSort(bson.M{"full": -1})
	}

	cursor, err := m.find(filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, os.ErrNotExist
		}
		return nil, err
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
			return nil, err
		}
		folders = append(folders, folder)
	}
	return folders, nil
}

func (m *metadata) SaveBlock(folderPaths []string, saveHandler func(folders map[string]*common.Folder) (bool, error)) error {
	folderPaths = m.cleanDuplicates(folderPaths)

	m.mutex.Wait(metadataLockKey)

	for i := range folderPaths {
		m.mutex.Lock(folderPaths[i])
	}
	defer func() {
		for _, folderPath := range folderPaths {
			m.mutex.Unlock(folderPath)
		}
	}()

	folders := make(map[string]*common.Folder)
	for _, folderPath := range folderPaths {
		folder, err := m.findOne(context.Background(), folderPath)
		if err != nil {
			return err
		}
		folders[folderPath] = folder
	}

	save, err := saveHandler(folders)
	if save {
		if err := m.overwrite(folders); err != nil {
			return err
		}
	}
	return err
}

func (m *metadata) SaveChain(folderPath string, saveHandler func(folder *common.Folder) (bool, error)) error {
	folderTree := common.PathTree(folderPath)

	m.mutex.Wait(metadataLockKey)

	folderTreeBackup := make([]string, len(folderTree))
	copy(folderTreeBackup, folderTree)

	droppedMutex := make(map[string]bool)
	for i := range folderTreeBackup {
		m.mutex.Lock(folderTreeBackup[i])
	}
	defer func() {
		for _, folderPath := range folderTreeBackup {
			if _, has := droppedMutex[folderPath]; has {
				continue
			}
			m.mutex.Unlock(folderPath)
		}
	}()

	insertOneFunc := func(parentContext context.Context, folder common.Folder) error {
		ctx, cancelFunc := m.context(parentContext)
		defer cancelFunc()

		_, err := m.col.InsertOne(ctx, folder)
		return err
	}

	session, err := m.conn.client.StartSession()
	if err != nil {
		return err
	}

	var folder *common.Folder

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

		var parentFolder *common.Folder
		for len(folderTree) > 0 {
			folderPath := folderTree[0]

			folder, err = m.findOne(parentContext, folderPath)
			if err != nil {
				if err != os.ErrNotExist {
					return err
				}

				if parentFolder == nil {
					parentFolder = common.NewFolder("/")

					if err := insertOneFunc(parentContext, *parentFolder); err != nil {
						return err
					}
					folderTree = folderTree[1:]

					continue
				}

				_, folderName := common.Split(folderPath)

				folder, err = parentFolder.NewFolder(folderName)
				if err != nil {
					if err == os.ErrExist {
						return errors.ErrRepair
					}
					return err
				}

				if err := m.updateOne(parentContext, parentFolder.Full, *parentFolder); err != nil {
					return err
				}
				if err := insertOneFunc(parentContext, *folder); err != nil {
					return err
				}
			}

			if len(folderTree) == 1 {
				break
			}

			parentFolder = folder
			folderTree = folderTree[1:]

			m.mutex.Unlock(parentFolder.Full)
			droppedMutex[parentFolder.Full] = true
		}

		return sc.CommitTransaction(parentContext)
	}); err != nil {
		return err
	}

	ctxS2, cancelS2Func := m.context(context.Background())
	defer cancelS2Func()

	session.EndSession(ctxS2)

	if folder == nil {
		return nil
	}

	save, err := saveHandler(folder)
	if !save {
		return err
	}

	if err := m.updateOne(context.Background(), folder.Full, *folder); err != nil {
		return err
	}

	return err
}

func (m *metadata) overwrite(folders map[string]*common.Folder) error {
	deleteOneFunc := func(parentContext context.Context, folderPath string) error {
		ctx, cancelFunc := m.context(parentContext)
		defer cancelFunc()

		if _, err := m.col.DeleteOne(ctx, bson.M{"full": folderPath}); err != nil {
			if err == mongo.ErrNoDocuments {
				return os.ErrNotExist
			}
			return err
		}
		return nil
	}

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

		for folderPath, folder := range folders {
			if folder == nil {
				if err := deleteOneFunc(parentContext, folderPath); err != nil && err != os.ErrNotExist {
					return err
				}
				continue
			}

			if err := m.updateOne(parentContext, folderPath, *folder); err != nil {
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

func (m *metadata) cleanDuplicates(folderPaths []string) []string {
	cleanedUps := make([]string, 0)

	for _, folderPath := range folderPaths {
		exists := false
		for _, cleanedUp := range cleanedUps {
			if strings.Compare(cleanedUp, folderPath) == 0 {
				exists = true
				break
			}
		}
		if !exists {
			cleanedUps = append(cleanedUps, folderPath)
		}
	}

	return cleanedUps
}

var _ Metadata = &metadata{}
