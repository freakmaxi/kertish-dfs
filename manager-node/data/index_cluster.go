package data

import "github.com/mediocregopher/radix/v3"

type indexCluster struct {
	cluster *radix.Cluster
}

func NewIndexClusterClient(addresses []string, password string) (IndexClient, error) {
	connFunc := func(network string, addr string) (radix.Conn, error) {
		return radix.Dial(
			network,
			addr,
			radix.DialAuthPass(password),
		)
	}
	poolFunc := func(network, addr string) (radix.Client, error) {
		return radix.NewPool(network, addr, 10, radix.PoolConnFunc(connFunc))
	}
	cluster, err := radix.NewCluster(addresses, radix.ClusterPoolFunc(poolFunc))
	if err != nil {
		return nil, err
	}

	return &indexCluster{
		cluster: cluster,
	}, nil
}

func (r indexCluster) Del(keys ...string) error {
	return r.cluster.Do(radix.Cmd(nil, "DEL", keys...))
}

func (r indexCluster) HSet(key, field string, value string) error {
	return r.cluster.Do(radix.Cmd(nil, "HSET", key, field, value))
}

func (r indexCluster) HGet(key, field string) (*string, error) {
	var result string
	value := radix.MaybeNil{
		Rcv: &result,
	}
	err := r.cluster.Do(radix.Cmd(&value, "HGET", key, field))
	if err != nil {
		return nil, err
	}
	if value.Nil {
		return nil, nil
	}
	return &result, nil
}

func (r indexCluster) HDel(key string, fields ...string) error {
	args := make([]string, 0)
	args = append(args, key)
	args = append(args, fields...)

	return r.cluster.Do(radix.Cmd(nil, "HDEL", args...))
}

func (r indexCluster) HGetAll(key string) (map[string]string, error) {
	var value map[string]string
	err := r.cluster.Do(radix.Cmd(&value, "HGETALL", key))
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (r indexCluster) HMSet(key string, values map[string]string) error {
	args := make([]string, 0)
	args = append(args, key)
	for k, v := range values {
		args = append(args, k)
		args = append(args, v)

		if len(args) > multiSetStepLimit {
			if err := r.cluster.Do(radix.Cmd(nil, "HMSET", args...)); err != nil {
				return err
			}
			args = make([]string, 0)
			args = append(args, key)
		}
	}
	if len(args) == 1 {
		return nil
	}
	return r.cluster.Do(radix.Cmd(nil, "HMSET", args...))
}

var _ IndexClient = &indexCluster{}
