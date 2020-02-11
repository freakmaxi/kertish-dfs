package data

import (
	"time"

	"github.com/go-redis/redis/v7"
)

type mutexCluster struct {
	cluster *redis.ClusterClient
}

func NewMutexClusterClient(addresses []string) (MutexClient, error) {
	cluster := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addresses,
	})

	_, err := cluster.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &mutexCluster{
		cluster: cluster,
	}, nil
}

func (m mutexCluster) Get(key string) *redis.StringCmd {
	return m.cluster.Get(key)
}

func (m mutexCluster) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return m.cluster.Set(key, value, expiration)
}

func (m mutexCluster) Del(keys ...string) *redis.IntCmd {
	return m.cluster.Del(keys...)
}

var _ MutexClient = &mutexCluster{}
