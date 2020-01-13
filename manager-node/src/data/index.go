package data

import (
	"fmt"

	"github.com/freakmaxi/2020-dfs/manager-node/src/errors"
	"github.com/go-redis/redis/v7"
)

type Index interface {
	Add(clusterId string, sha512Hex string) error
	Find(clusterIds []string, sha512Hex string) (string, error)
	Remove(clusterId string, sha512Hex string) error
	Replace(clusterId string, sha512HexList []string) error
	Compare(clusterId string, sha512HexList []string) (uint64, error)
}

type index struct {
	mutex     Mutex
	client    *redis.Client
	keyPrefix string
}

func NewIndex(address string, keyPrefix string, mutex Mutex) (Index, error) {
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &index{
		mutex:     mutex,
		client:    client,
		keyPrefix: keyPrefix,
	}, nil
}

func (i *index) key(name string) string {
	return fmt.Sprintf("%s_index_%s", i.keyPrefix, name)
}

func (i *index) Add(clusterId string, sha512Hex string) error {
	return i.lock(clusterId, func(index map[string]string) error {
		index[sha512Hex] = clusterId
		return nil
	})
}

func (i *index) Find(clusterIds []string, sha512Hex string) (string, error) {
	for _, clusterId := range clusterIds {
		indexKey := i.key(clusterId)
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
	return i.lock(clusterId, func(index map[string]string) error {
		delete(index, sha512Hex)
		return nil
	})
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
