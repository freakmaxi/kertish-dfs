package manager

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

func md5Hash(v string) string {
	hash := md5.New()
	_, _ = hash.Write([]byte(v))

	return hex.EncodeToString(hash.Sum(nil))
}

func newClusterId() string {
	clusterId := uuid.New()
	return md5Hash(clusterId.String())
}

func newNodeId(hardwareAddr string, bindAddr string, size uint64) string {
	colonIdx := strings.LastIndex(bindAddr, ":")
	if colonIdx > -1 {
		bindAddr = bindAddr[colonIdx+1:]
	} else {
		bindAddr = "9430"
	}
	nodeId := fmt.Sprintf("%s%s%s", hardwareAddr, bindAddr, strconv.FormatUint(size, 10))
	return md5Hash(nodeId)
}
