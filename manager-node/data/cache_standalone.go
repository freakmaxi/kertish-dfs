package data

import "github.com/mediocregopher/radix/v3"

type cacheStandalone struct {
	client *radix.Pool
}

func NewCacheStandaloneClient(address string, password string) (CacheClient, error) {
	connFunc := func(network string, addr string) (radix.Conn, error) {
		return radix.Dial(
			network,
			addr,
			radix.DialAuthPass(password),
		)
	}
	client, err := radix.NewPool("tcp", address, 10, radix.PoolConnFunc(connFunc))
	if err != nil {
		return nil, err
	}

	return &cacheStandalone{
		client: client,
	}, nil
}

func (r cacheStandalone) Del(keys ...string) error {
	return r.client.Do(radix.Cmd(nil, "DEL", keys...))
}

func (r cacheStandalone) HSet(key, field string, value string) error {
	return r.client.Do(radix.Cmd(nil, "HSET", key, field, value))
}

func (r cacheStandalone) HGet(key, field string) (*string, error) {
	var result string
	value := radix.MaybeNil{
		Rcv: &result,
	}
	if err := r.client.Do(radix.Cmd(&value, "HGET", key, field)); err != nil {
		return nil, err
	}
	if value.Nil {
		return nil, nil
	}
	return &result, nil
}

func (r cacheStandalone) HDel(key string, fields ...string) error {
	args := make([]string, 0)
	args = append(args, key)
	args = append(args, fields...)

	return r.client.Do(radix.Cmd(nil, "HDEL", args...))
}

func (r cacheStandalone) HGetAll(key string) (map[string]string, error) {
	var result map[string]string
	value := radix.MaybeNil{
		Rcv: &result,
	}
	err := r.client.Do(radix.Cmd(&value, "HGETALL", key))
	if err != nil {
		return nil, err
	}
	if value.Nil {
		return nil, nil
	}
	return result, nil
}

func (r cacheStandalone) HMSet(key string, values map[string]string) error {
	args := make([]string, 0)
	args = append(args, key)
	for k, v := range values {
		args = append(args, k)
		args = append(args, v)

		if len(args) > multiSetStepLimit {
			if err := r.client.Do(radix.Cmd(nil, "HMSET", args...)); err != nil {
				return err
			}
			args = make([]string, 0)
			args = append(args, key)
		}
	}
	if len(args) == 1 {
		return nil
	}
	return r.client.Do(radix.Cmd(nil, "HMSET", args...))
}

func (r cacheStandalone) Pipeline(commands []radix.CmdAction) error {
	if len(commands) == 0 {
		return nil
	}
	return r.client.Do(radix.Pipeline(commands...))
}

func (r cacheStandalone) Get(key string) (*string, error) {
	var result string
	value := radix.MaybeNil{
		Rcv: &result,
	}
	if err := r.client.Do(radix.Cmd(&value, "GET", key)); err != nil {
		return nil, err
	}
	if value.Nil {
		return nil, nil
	}
	return &result, nil
}

func (r cacheStandalone) Set(key string, value string) error {
	return r.client.Do(radix.Cmd(nil, "SET", key, value))
}

func (r cacheStandalone) Do(cmd radix.CmdAction) error {
	return r.client.Do(cmd)
}

var _ CacheClient = &cacheStandalone{}
