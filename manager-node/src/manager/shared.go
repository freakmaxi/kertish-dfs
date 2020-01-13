package manager

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/google/uuid"
)

func md5Hash(v string) string {
	hash := md5.New()
	hash.Write([]byte(v))

	return hex.EncodeToString(hash.Sum(nil))
}

func newClusterId() string {
	clusterId := uuid.New()
	return md5Hash(clusterId.String())
}

func newNodeId(bindAddr string, size uint64) string {
	nodeId := fmt.Sprintf("%s%s", bindAddr, strconv.FormatUint(size, 10))
	return md5Hash(nodeId)
}
