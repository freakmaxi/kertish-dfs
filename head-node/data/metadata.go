package data

import (
	"context"
	"fmt"
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
	dfsCol := conn.db.Database(database).Collection(metadataCollection)

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

func (m *metadata) context(sc context.Context) context.Context {
	if sc != nil && m.conn.transaction {
		return sc
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	return ctx
}

func (m *metadata) setupIndices() error {
	model := mongo.IndexModel{
		Keys: bson.M{"full": 1},
	}
	_, err := m.col.Indexes().CreateOne(m.context(nil), model, nil)
	return err
}

func (m *metadata) Get(folderPaths []string) ([]*common.Folder, error) {
	folderPaths = m.cleanDuplicates(folderPaths)

	folders := make([]*common.Folder, 0)
	for _, folderPath := range folderPaths {
		var folder *common.Folder
		if err := m.col.FindOne(m.context(nil), bson.M{"full": folderPath}).Decode(&folder); err != nil {
			if err == mongo.ErrNoDocuments {
				return nil, os.ErrNotExist
			}
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

	cursor, err := m.col.Find(m.context(nil), filter, opts)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	defer func() { _ = cursor.Close(m.context(nil)) }()

	folders := make([]*common.Folder, 0)
	for cursor.Next(m.context(nil)) {
		var folder *common.Folder
		if err := cursor.Decode(&folder); err != nil {
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
		var folder *common.Folder
		if err := m.col.FindOne(m.context(nil), bson.M{"full": folderPath}).Decode(&folder); err != nil && err != mongo.ErrNoDocuments {
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

func (m *metadata) SaveChain(folderPath string, saveHandler func(folder *common.Folder) (bool, error)) (resultErr error) {
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

	var folder *common.Folder

	session, err := m.conn.db.StartSession()
	if err != nil {
		return err
	}

	if err = mongo.WithSession(m.context(nil), session, func(sc mongo.SessionContext) error {
		if err = sc.StartTransaction(); err != nil {
			return err
		}

		var parentFolder *common.Folder
		for len(folderTree) > 0 {
			folderPath := folderTree[0]

			if err := m.col.FindOne(m.context(sc), bson.M{"full": folderPath}).Decode(&folder); err != nil {
				if err != mongo.ErrNoDocuments {
					return err
				}

				if parentFolder == nil {
					parentFolder = common.NewFolder("/")

					if _, err := m.col.InsertOne(m.context(sc), parentFolder); err != nil {
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

				opts := (&options.UpdateOptions{}).SetUpsert(true)
				if _, err := m.col.UpdateOne(m.context(sc), bson.M{"full": parentFolder.Full}, bson.M{"$set": parentFolder}, opts); err != nil {
					return err
				}
				if _, err := m.col.InsertOne(m.context(sc), folder); err != nil {
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

		return sc.CommitTransaction(m.context(sc))
	}); err != nil {
		return err
	}

	session.EndSession(m.context(nil))

	if folder == nil {
		return
	}

	var save bool
	save, resultErr = saveHandler(folder)
	if !save {
		return
	}

	opts := (&options.UpdateOptions{}).SetUpsert(true)
	if _, err := m.col.UpdateOne(m.context(nil), bson.M{"full": folder.Full}, bson.M{"$set": folder}, opts); err != nil {
		return err
	}

	return
}

func (m *metadata) matchTree(folderTree []string) ([]string, error) {
	folderTree = m.cleanDuplicates(folderTree)

	m.mutex.Wait(metadataLockKey)

	for i := range folderTree {
		m.mutex.Lock(folderTree[i])
	}
	defer func() {
		for _, folderPath := range folderTree {
			m.mutex.Unlock(folderPath)
		}
	}()

	matches := make([]string, 0)
	for _, folderPath := range folderTree {
		if err := m.col.FindOne(m.context(nil), bson.M{"full": folderPath}).Err(); err != nil {
			if err == mongo.ErrNoDocuments {
				break
			}
			return nil, err
		}

		matches = append(matches, folderPath)
	}

	return matches, nil
}

func (m *metadata) overwrite(folders map[string]*common.Folder) error {
	session, err := m.conn.db.StartSession()
	if err != nil {
		return err
	}

	if err = mongo.WithSession(m.context(nil), session, func(sc mongo.SessionContext) error {
		if err = sc.StartTransaction(); err != nil {
			return err
		}

		for folderPath, folder := range folders {
			filter := bson.M{"full": folderPath}

			if folder == nil {
				if _, err := m.col.DeleteOne(m.context(sc), filter); err != nil && err != mongo.ErrNoDocuments {
					return err
				}
				continue
			}

			opts := (&options.UpdateOptions{}).SetUpsert(true)
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

func (m *metadata) filterFolderTree(matches []string, folderTree []string) []string {
	if len(matches) == 0 {
		return folderTree
	}

	for len(folderTree) > 0 {
		if strings.Compare(matches[len(matches)-1], folderTree[0]) == 0 {
			break
		}
		folderTree = folderTree[1:]
	}

	return folderTree
}

var _ Metadata = &metadata{}
