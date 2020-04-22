package data

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
)

type MutexClient interface {
	Get(key string) *redis.StringCmd
	Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(keys ...string) *redis.IntCmd
}

type Mutex interface {
	Lock(string)
	UnLock(string)
	Wait(string)
}

type mutex struct {
	client    MutexClient
	keyPrefix string
}

func NewMutex(client MutexClient) Mutex {
	return &mutex{
		client:    client,
		keyPrefix: "lock",
	}
}

func (m *mutex) key(name string) string {
	return fmt.Sprintf("%s-%s", m.keyPrefix, name)
}

func (m *mutex) Lock(name string) {
	lockKey := m.key(name)
	for {
		if _, err := m.client.Get(lockKey).Int(); err == nil {
			time.Sleep(time.Millisecond * 10)
			continue
		}
		m.client.Set(lockKey, 0, time.Hour*24*7)
		break
	}
}

func (m *mutex) UnLock(name string) {
	m.client.Del(m.key(name))
}

func (m *mutex) Wait(name string) {
	lockKey := m.key(name)
	for {
		if _, err := m.client.Get(lockKey).Int(); err == nil {
			time.Sleep(time.Millisecond * 10)
			continue
		}
		break
	}
}

var _ Mutex = &mutex{}
