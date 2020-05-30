package data

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/mediocregopher/radix/v3"
)

const multiSetStepLimit = 50000

type Index interface {
	Add(clusterId string, fileItem common.SyncFileItem) error
	AddBulk(clusterId string, fileItemList common.SyncFileItemList) error
	Find(clusterIds []string, sha512Hex string) (string, *common.SyncFileItem, error)
	List(clusterId string) (common.SyncFileItemList, error)
	Remove(clusterId string, sha512Hex string) error
	RemoveBulk(clusterId string, sha512HexList []string) error
	Replace(clusterId string, fileItemList common.SyncFileItemList) error
	Compare(clusterId string, fileItemList common.SyncFileItemList) (uint64, error)
	Extract(clusterId string, fileItemList common.SyncFileItemList) (common.SyncFileItemList, error)
}

type index struct {
	mutex *sync.Mutex

	client    CacheClient
	keyPrefix string
}

func NewIndex(client CacheClient, keyPrefix string) Index {
	return &index{
		client:    client,
		keyPrefix: keyPrefix,
		mutex:     &sync.Mutex{},
	}
}

func (i *index) key(name string) string {
	return fmt.Sprintf("%s_index_%s", i.keyPrefix, name)
}

func (i *index) Add(clusterId string, fileItem common.SyncFileItem) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	return i.client.HSet(i.key(clusterId), fileItem.Sha512Hex, fmt.Sprintf("%s|%d", clusterId, fileItem.Size))
}

func (i *index) AddBulk(clusterId string, fileItemList common.SyncFileItemList) error {
	if len(fileItemList) == 0 {
		return nil
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()

	commands := make([]radix.CmdAction, 0)
	for _, fileItem := range fileItemList {
		commands = append(commands,
			radix.Cmd(nil, "HSET", i.key(clusterId),
				fileItem.Sha512Hex, fmt.Sprintf("%s|%d", clusterId, fileItem.Size)))
	}

	return i.client.Pipeline(commands)
}

func (i *index) Find(clusterIds []string, sha512Hex string) (string, *common.SyncFileItem, error) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	for _, clusterId := range clusterIds {
		chunkInfo, err := i.client.HGet(i.key(clusterId), sha512Hex)
		if err != nil {
			return "", nil, err
		}
		if chunkInfo == nil {
			continue
		}

		parts := strings.Split(*chunkInfo, "|")
		if len(parts) != 2 {
			return "", nil, errors.ErrNotFound
		}

		size, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return "", nil, err
		}

		return parts[0], &common.SyncFileItem{Sha512Hex: sha512Hex, Size: int32(size)}, nil
	}
	return "", nil, errors.ErrNotFound
}

func (i *index) List(clusterId string) (common.SyncFileItemList, error) {
	fileItemList := make(common.SyncFileItemList, 0)

	if err := i.lock(clusterId, func(index map[string]string) error {
		for k, v := range index {
			parts := strings.Split(v, "|")
			if len(parts) != 2 {
				continue
			}

			size, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				continue
			}

			fileItemList = append(fileItemList, common.SyncFileItem{
				Sha512Hex: k,
				Size:      int32(size),
			})
		}

		return os.ErrInvalid
	}); err != nil && err != os.ErrInvalid {
		return nil, err
	}

	return fileItemList, nil
}

func (i *index) Remove(clusterId string, sha512Hex string) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	return i.client.HDel(i.key(clusterId), sha512Hex)
}

func (i *index) RemoveBulk(clusterId string, sha512HexList []string) error {
	if len(sha512HexList) == 0 {
		return nil
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()

	commands := make([]radix.CmdAction, 0)
	for _, sha512Hex := range sha512HexList {
		commands = append(commands,
			radix.Cmd(nil, "HDEL", i.key(clusterId), sha512Hex))
	}

	return i.client.Pipeline(commands)
}

func (i *index) Replace(clusterId string, fileItemList common.SyncFileItemList) error {
	if fileItemList == nil {
		fileItemList = make(common.SyncFileItemList, 0)
	}

	return i.lock(clusterId, func(index map[string]string) error {
		for k := range index {
			delete(index, k)
		}

		for _, fileItem := range fileItemList {
			index[fileItem.Sha512Hex] = fmt.Sprintf("%s|%d", clusterId, fileItem.Size)
		}
		return nil
	})
}

func (i *index) Compare(clusterId string, fileItemList common.SyncFileItemList) (uint64, error) {
	failed := uint64(0)
	err := i.lock(clusterId, func(index map[string]string) error {
		indexShadow := make(map[string]string)
		for k, v := range index {
			indexShadow[k] = v
		}

		for _, fileItem := range fileItemList {
			delete(indexShadow, fileItem.Sha512Hex)
		}
		failed = uint64(len(indexShadow))

		return nil
	})

	return failed, err
}

func (i *index) Extract(clusterId string, fileItemList common.SyncFileItemList) (common.SyncFileItemList, error) {
	var extractedList common.SyncFileItemList
	err := i.lock(clusterId, func(index map[string]string) error {
		indexShadow := make(map[string]string)
		for k, v := range index {
			indexShadow[k] = v
		}

		for _, fileItem := range fileItemList {
			delete(indexShadow, fileItem.Sha512Hex)
		}

		extractedList = make(common.SyncFileItemList, 0)
		for k, v := range indexShadow {
			parts := strings.Split(v, "|")
			if len(parts) != 2 {
				continue
			}

			size, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				continue
			}

			extractedList = append(extractedList, common.SyncFileItem{
				Sha512Hex: k,
				Size:      int32(size),
			})
		}

		return nil
	})
	return extractedList, err
}

func (i index) lock(clusterId string, lockHandler func(index map[string]string) error) error {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	indexKey := i.key(clusterId)
	index, err := i.client.HGetAll(indexKey)
	if err != nil {
		return err
	}
	if index == nil {
		index = make(map[string]string, 0)
	}

	if err := lockHandler(index); err != nil {
		return err
	}

	if err := i.client.Del(indexKey); err != nil {
		return err
	}

	if len(index) > 0 {
		return i.client.HMSet(indexKey, index)
	}

	return nil
}

var _ Index = &index{}
