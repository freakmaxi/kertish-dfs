package data

import "github.com/go-redis/redis/v7"

type indexCluster struct {
	cluster *redis.ClusterClient
}

func NewIndexClusterClient(addresses []string, password string) (IndexClient, error) {
	cluster := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addresses,
		Password: password,
	})

	_, err := cluster.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &indexCluster{
		cluster: cluster,
	}, nil
}

func (r indexCluster) Del(keys ...string) *redis.IntCmd {
	return r.cluster.Del(keys...)
}

func (r indexCluster) HSet(key, field string, value interface{}) *redis.IntCmd {
	return r.cluster.HSet(key, field, value)
}

func (r indexCluster) HGet(key, field string) *redis.StringCmd {
	return r.cluster.HGet(key, field)
}

func (r indexCluster) HDel(key string, fields ...string) *redis.IntCmd {
	return r.cluster.HDel(key, fields...)
}

func (r indexCluster) HGetAll(key string) *redis.StringStringMapCmd {
	return r.cluster.HGetAll(key)
}

func (r indexCluster) HMSet(key string, values ...interface{}) *redis.BoolCmd {
	return r.cluster.HMSet(key, values...)
}

var _ IndexClient = &indexCluster{}
