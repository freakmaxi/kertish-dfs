package data

import (
	"time"

	"github.com/go-redis/redis/v7"
)

type mutexStandalone struct {
	client *redis.Client
}

func NewMutexStandaloneClient(address string) (MutexClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &mutexStandalone{
		client: client,
	}, nil
}

func (m mutexStandalone) Get(key string) *redis.StringCmd {
	return m.client.Get(key)
}

func (m mutexStandalone) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return m.client.Set(key, value, expiration)
}

func (m mutexStandalone) Del(keys ...string) *redis.IntCmd {
	return m.client.Del(keys...)
}

var _ MutexClient = &mutexStandalone{}
