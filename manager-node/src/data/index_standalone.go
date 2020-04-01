package data

import "github.com/go-redis/redis/v7"

type indexStandalone struct {
	client *redis.Client
}

func NewIndexStandaloneClient(address string, password string) (IndexClient, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
	})

	_, err := client.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &indexStandalone{
		client: client,
	}, nil
}

func (r indexStandalone) Del(keys ...string) *redis.IntCmd {
	return r.client.Del(keys...)
}

func (r indexStandalone) HSet(key, field string, value interface{}) *redis.BoolCmd {
	return r.client.HSet(key, field, value)
}

func (r indexStandalone) HGet(key, field string) *redis.StringCmd {
	return r.client.HGet(key, field)
}

func (r indexStandalone) HDel(key string, fields ...string) *redis.IntCmd {
	return r.client.HDel(key, fields...)
}

func (r indexStandalone) HGetAll(key string) *redis.StringStringMapCmd {
	return r.client.HGetAll(key)
}

func (r indexStandalone) HMSet(key string, values ...interface{}) *redis.IntCmd {
	return r.client.HMSet(key, values...)
}

var _ IndexClient = &indexStandalone{}
