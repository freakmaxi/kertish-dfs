package data

import (
	"fmt"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/mediocregopher/radix/v3"
)

const multiSetStepLimit = 50000

type IndexClient interface {
	Del(keys ...string) error
	HSet(key, field string, value string) error
	HGet(key, field string) (*string, error)
	HDel(key string, fields ...string) error
	HGetAll(key string) (map[string]string, error)
	HMSet(key string, values map[string]string) error
	Pipeline(commands []radix.CmdAction) error
}

type Index interface {
	Add(clusterId string, sha512Hex string) error
	AddBulk(clusterId string, sha512HexList []string) error
	Find(clusterIds []string, sha512Hex string) (string, error)
	Remove(clusterId string, sha512Hex string) error
	RemoveBulk(clusterId string, sha512HexList []string) error
	Replace(clusterId string, fileItemList common.SyncFileItems) error
	Compare(clusterId string, fileItemList common.SyncFileItems) (uint64, error)
}

type index struct {
	mutex *sync.Mutex

	client    IndexClient
	keyPrefix string
}

func NewIndex(client IndexClient, keyPrefix string) Index {
	return &index{
		client:    client,
		keyPrefix: keyPrefix,
		mutex:     &sync.Mutex{},
	}
}

func (i *index) key(name string) string {
	return fmt.Sprintf("%s_index_%s", i.keyPrefix, name)
}

func (i *index) Add(clusterId string, sha512Hex string) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	return i.client.HSet(i.key(clusterId), sha512Hex, clusterId)
}

func (i *index) AddBulk(clusterId string, sha512HexList []string) error {
	if len(sha512HexList) == 0 {
		return nil
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()

	commands := make([]radix.CmdAction, 0)
	for _, sha512Hex := range sha512HexList {
		commands = append(commands,
			radix.Cmd(nil, "HSET", i.key(clusterId), sha512Hex, clusterId))
	}

	return i.client.Pipeline(commands)
}

func (i *index) Find(clusterIds []string, sha512Hex string) (string, error) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	for _, clusterId := range clusterIds {
		clusterIdBackup, err := i.client.HGet(i.key(clusterId), sha512Hex)
		if err != nil {
			return "", err
		}
		if clusterIdBackup == nil {
			continue
		}
		return *clusterIdBackup, nil
	}
	return "", errors.ErrNotFound
}

func (i *index) Remove(clusterId string, sha512Hex string) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	return i.client.HDel(i.key(clusterId), sha512Hex)
}

func (i *index) RemoveBulk(clusterId string, sha512HexList []string) error {
	if len(sha512HexList) == 0 {
		return nil
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()

	commands := make([]radix.CmdAction, 0)
	for _, sha512Hex := range sha512HexList {
		commands = append(commands,
			radix.Cmd(nil, "HDEL", i.key(clusterId), sha512Hex))
	}

	return i.client.Pipeline(commands)
}

func (i *index) Replace(clusterId string, fileItemList common.SyncFileItems) error {
	if fileItemList == nil {
		fileItemList = make(common.SyncFileItems, 0)
	}

	return i.lock(clusterId, func(index map[string]string) error {
		for k := range index {
			delete(index, k)
		}

		for _, fileItem := range fileItemList {
			index[fileItem.Sha512Hex] = clusterId
		}
		return nil
	})
}

func (i *index) Compare(clusterId string, fileItemList common.SyncFileItems) (uint64, error) {
	failed := uint64(0)
	err := i.lock(clusterId, func(index map[string]string) error {
		indexShadow := make(map[string]string)
		for k, v := range index {
			indexShadow[k] = v
		}

		for _, fileItem := range fileItemList {
			delete(indexShadow, fileItem.Sha512Hex)
		}
		failed = uint64(len(indexShadow))

		return nil
	})

	return failed, err
}

func (i index) lock(clusterId string, lockHandler func(index map[string]string) error) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	indexKey := i.key(clusterId)
	index, err := i.client.HGetAll(indexKey)
	if err != nil {
		return err
	}
	if index == nil {
		index = make(map[string]string, 0)
	}

	if err := lockHandler(index); err != nil {
		return err
	}

	if err := i.client.Del(indexKey); err != nil {
		return err
	}

	if len(index) > 0 {
		return i.client.HMSet(indexKey, index)
	}

	return nil
}

var _ Index = &index{}
