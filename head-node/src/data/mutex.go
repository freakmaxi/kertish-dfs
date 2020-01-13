package data

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v7"
)

type Mutex interface {
	Lock(string)
	UnLock(string)
	Wait(string)
}

type mutex struct {
	client    *redis.Client
	keyPrefix string
}

func NewMutex(address string) (Mutex, error) {
	client := redis.NewClient(&redis.Options{
		Addr: address,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &mutex{
		client:    client,
		keyPrefix: "lock",
	}, nil
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
