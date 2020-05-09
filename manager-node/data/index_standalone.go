package data

import "github.com/mediocregopher/radix/v3"

type indexStandalone struct {
	client *radix.Pool
}

func NewIndexStandaloneClient(address string, password string) (IndexClient, error) {
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

	return &indexStandalone{
		client: client,
	}, nil
}

func (r indexStandalone) Del(keys ...string) error {
	return r.client.Do(radix.Cmd(nil, "DEL", keys...))
}

func (r indexStandalone) HSet(key, field string, value string) error {
	return r.client.Do(radix.Cmd(nil, "HSET", key, field, value))
}

func (r indexStandalone) HGet(key, field string) (*string, error) {
	var result string
	value := radix.MaybeNil{
		Rcv: &result,
	}
	err := r.client.Do(radix.Cmd(&value, "HGET", key, field))
	if err != nil {
		return nil, err
	}
	if value.Nil {
		return nil, nil
	}
	return &result, nil
}

func (r indexStandalone) HDel(key string, fields ...string) error {
	args := make([]string, 0)
	args = append(args, key)
	args = append(args, fields...)

	return r.client.Do(radix.Cmd(nil, "HDEL", args...))
}

func (r indexStandalone) HGetAll(key string) (map[string]string, error) {
	var value map[string]string
	err := r.client.Do(radix.Cmd(&value, "HGETALL", key))
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (r indexStandalone) HMSet(key string, values map[string]string) error {
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

var _ IndexClient = &indexStandalone{}
