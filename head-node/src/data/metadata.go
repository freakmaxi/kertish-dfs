package data

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/head-node/src/common"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Metadata interface {
	Lock(folderPaths []string, folderHandler func(folders []*common.Folder) error) error
	LockChildrenOf(folderPath string, folderHandler func(folders []*common.Folder) error) error
	Save(folderPaths []string, saveHandler func(folders map[string]*common.Folder) error) error
}

const collection = "metadata"

type metadata struct {
	mutex Mutex
	conn  *Connection
	col   *mongo.Collection
}

func NewMetadata(mutex Mutex, conn *Connection, database string) (Metadata, error) {
	dfsCol := conn.db.Database(database).Collection(collection)

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

func (m *metadata) context() context.Context {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*30)
	return ctx
}

func (m *metadata) setupIndices() error {
	model := mongo.IndexModel{
		Keys: bson.M{"full": 1},
	}
	_, err := m.col.Indexes().CreateOne(m.context(), model, nil)
	return err
}

func (m *metadata) Lock(folderPaths []string, folderHandler func(folders []*common.Folder) error) error {
	folderPaths = m.cleanDuplicates(folderPaths)

	for i := range folderPaths {
		m.mutex.Wait(folderPaths[i])
	}

	folders := make([]*common.Folder, 0)
	for _, folderPath := range folderPaths {
		var folder *common.Folder
		if err := m.col.FindOne(m.context(), bson.M{"full": folderPath}).Decode(&folder); err != nil {
			if err == mongo.ErrNoDocuments {
				return os.ErrNotExist
			}
			return err
		}

		folders = append(folders, folder)
	}

	return folderHandler(folders)
}

func (m *metadata) LockChildrenOf(folderPath string, folderHandler func(folders []*common.Folder) error) error {
	keyword := fmt.Sprintf("^%s/.+", folderPath)
	filter := bson.M{"full": bson.M{"$regex": primitive.Regex{Pattern: keyword}}}
	cursor, err := m.col.Find(m.context(), filter)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return os.ErrNotExist
		}
		return err
	}

	folders := make([]*common.Folder, 0)
	for cursor.Next(m.context()) {
		var folder *common.Folder
		if err := cursor.Decode(&folder); err != nil {
			return err
		}
		folders = append(folders, folder)
		m.mutex.Wait(folder.Full)
	}

	return folderHandler(folders)
}

func (m *metadata) Save(folderPaths []string, saveHandler func(folders map[string]*common.Folder) error) error {
	folderPaths = m.cleanDuplicates(folderPaths)

	for i := range folderPaths {
		m.mutex.Lock(folderPaths[i])
	}
	defer func() {
		for _, folderPath := range folderPaths {
			m.mutex.UnLock(folderPath)
		}
	}()

	folders := make(map[string]*common.Folder)
	for _, folderPath := range folderPaths {
		var folder *common.Folder
		if err := m.col.FindOne(m.context(), bson.M{"full": folderPath}).Decode(&folder); err != nil && err != mongo.ErrNoDocuments {
			return err
		}
		folders[folderPath] = folder
	}

	if err := saveHandler(folders); err != nil {
		return err
	}

	return m.overwrite(folders)
}

func (m *metadata) overwrite(folders map[string]*common.Folder) error {
	session, err := m.conn.db.StartSession()
	if err != nil {
		return err
	}
	if err = session.StartTransaction(); err != nil {
		return err
	}

	if err = mongo.WithSession(m.context(), session, func(sc mongo.SessionContext) error {
		for k, folder := range folders {
			filter := bson.M{"full": k}

			if folder == nil {
				if _, err := m.col.DeleteOne(m.context(), filter); err != nil && err != mongo.ErrNoDocuments {
					return err
				}
				continue
			}

			opts := (&options.UpdateOptions{}).SetUpsert(true)
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
