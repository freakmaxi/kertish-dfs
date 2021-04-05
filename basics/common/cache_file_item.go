package common

import (
	"strconv"
	"time"
)

const cacheExpiresIn = time.Hour * 60

type CacheFileItem struct {
	FileItem  SyncFileItem
	ClusterId string
	ExistsIn  CacheFileItemLocationMap
	ExpiresAt time.Time
}

type CacheFileItemMap map[string]*CacheFileItem
type CacheFileItemLocationMap map[string]bool

func NewCacheFileItem(clusterId string, nodeId string, fileItem SyncFileItem) *CacheFileItem {
	existsIn := make(CacheFileItemLocationMap)
	existsIn[nodeId] = true

	return &CacheFileItem{
		FileItem: SyncFileItem{
			Sha512Hex: fileItem.Sha512Hex,
			Usage:     fileItem.Usage,
			Size:      fileItem.Size,
			Shadow:    fileItem.Shadow,
		},
		ClusterId: clusterId,
		ExistsIn:  existsIn,
		ExpiresAt: time.Now().UTC().Add(cacheExpiresIn),
	}
}

func NewCacheFileItemFromMap(cache map[string]string) *CacheFileItem {
	usage, _ := strconv.ParseUint(cache["usage"], 10, 64)
	size, _ := strconv.ParseUint(cache["size"], 10, 64)
	expireAt, _ := time.Parse(time.RFC3339, cache["expiresAt"])

	return &CacheFileItem{
		FileItem: SyncFileItem{
			Sha512Hex: cache["sha512Hex"],
			Usage:     uint16(usage),
			Size:      uint32(size),
		},
		ClusterId: cache["clusterId"],
		ExistsIn:  make(CacheFileItemLocationMap),
		ExpiresAt: expireAt,
	}
}

func (c *CacheFileItem) Export() map[string]string {
	export := make(map[string]string)

	export["sha512Hex"] = c.FileItem.Sha512Hex
	export["usage"] = strconv.FormatUint(uint64(c.FileItem.Usage), 10)
	export["size"] = strconv.FormatUint(uint64(c.FileItem.Size), 10)

	export["clusterId"] = c.ClusterId

	c.ExpiresAt = time.Now().UTC().Add(cacheExpiresIn)
	export["expiresAt"] = c.ExpiresAt.Format(time.RFC3339)

	return export
}

func (c *CacheFileItem) Expired() bool {
	return time.Now().UTC().After(c.ExpiresAt)
}
