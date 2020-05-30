package data

import "github.com/mediocregopher/radix/v3"

type CacheClient interface {
	Del(keys ...string) error
	HSet(key string, field string, value string) error
	HGet(key string, field string) (*string, error)
	HDel(key string, fields ...string) error
	HGetAll(key string) (map[string]string, error)
	HMSet(key string, values map[string]string) error
	Pipeline(commands []radix.CmdAction) error

	Get(key string) (*string, error)
	Set(key string, value string) error
}
