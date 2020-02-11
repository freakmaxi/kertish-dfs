package data

import (
	"fmt"

	"github.com/freakmaxi/kertish-dfs/manager-node/src/errors"
	"github.com/go-redis/redis/v7"
)

type IndexClient interface {
	Del(keys ...string) *redis.IntCmd
	HSet(key, field string, value interface{}) *redis.BoolCmd
	HGet(key, field string) *redis.StringCmd
	HDel(key string, fields ...string) *redis.IntCmd
	HGetAll(key string) *redis.StringStringMapCmd
	HMSet(key string, values ...interface{}) *redis.IntCmd
}

type Index interface {
	Add(clusterId string, sha512Hex string) error
	Find(clusterIds []string, sha512Hex string) (string, error)
	Remove(clusterId string, sha512Hex string) error
	Replace(clusterId string, sha512HexList []string) error
	Compare(clusterId string, sha512HexList []string) (uint64, error)
}

type index struct {
	mutex     Mutex
	client    IndexClient
	keyPrefix string
}

func NewIndex(client IndexClient, keyPrefix string, mutex Mutex) Index {
	return &index{
		client:    client,
		keyPrefix: keyPrefix,
		mutex:     mutex,
	}
}

func (i *index) key(name string) string {
	return fmt.Sprintf("%s_index_%s", i.keyPrefix, name)
}

func (i *index) Add(clusterId string, sha512Hex string) error {
	indexKey := i.key(clusterId)
	i.mutex.Wait(indexKey)

	return i.client.HSet(indexKey, sha512Hex, clusterId).Err()
}

func (i *index) Find(clusterIds []string, sha512Hex string) (string, error) {
	for _, clusterId := range clusterIds {
		indexKey := i.key(clusterId)
		i.mutex.Wait(indexKey)

		clusterIdBackup, err := i.client.HGet(indexKey, sha512Hex).Result()
		if err != nil {
			if err != redis.Nil {
				return "", err
			}
			continue
		}
		return clusterIdBackup, nil
	}
	return "", errors.ErrNotFound
}

func (i *index) Remove(clusterId string, sha512Hex string) error {
	indexKey := i.key(clusterId)
	i.mutex.Wait(indexKey)

	return i.client.HDel(indexKey, sha512Hex).Err()
}

func (i *index) Replace(clusterId string, sha512HexList []string) error {
	return i.lock(clusterId, func(index map[string]string) error {
		for k := range index {
			delete(index, k)
		}

		for _, sha512Hex := range sha512HexList {
			index[sha512Hex] = clusterId
		}
		return nil
	})
}

func (i *index) Compare(clusterId string, sha512HexList []string) (uint64, error) {
	failed := uint64(0)
	err := i.lock(clusterId, func(index map[string]string) error {
		indexShadow := make(map[string]string)
		for k, v := range index {
			indexShadow[k] = v
		}

		for _, sha512Hex := range sha512HexList {
			delete(indexShadow, sha512Hex)
		}
		failed = uint64(len(indexShadow))

		return nil
	})

	return failed, err
}

func (i index) lock(clusterId string, lockHandler func(index map[string]string) error) error {
	indexKey := i.key(clusterId)

	i.mutex.Lock(indexKey)
	defer i.mutex.UnLock(indexKey)

	index, err := i.client.HGetAll(indexKey).Result()
	if err != nil {
		if err != redis.Nil {
			return err
		}
		index = make(map[string]string, 0)
	}

	if err := lockHandler(index); err != nil {
		return err
	}

	indexShadow := make([]interface{}, len(index)*2)
	shadowIndex := 0
	for k, v := range index {
		indexShadow[shadowIndex] = k
		indexShadow[shadowIndex+1] = v
		shadowIndex += 2
	}

	if err := i.client.Del(indexKey).Err(); err != nil && err != redis.Nil {
		return err
	}

	if len(indexShadow) > 0 {
		return i.client.HMSet(indexKey, indexShadow...).Err()
	}

	return nil
}

var _ Index = &index{}
