package routing

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/head-node/manager"
	"go.uber.org/zap"
)

func (d *dfsRouter) handleGet(w http.ResponseWriter, r *http.Request) {
	requestedPaths, sourceAction, err := d.describeXPath(r.Header.Get("X-Path"))
	if err != nil {
		w.WriteHeader(422)
		return
	}

	read, err := d.dfs.Read(requestedPaths, strings.Compare(sourceAction, "j") == 0)
	if err != nil {
		if err == os.ErrNotExist {
			w.WriteHeader(404)
			return
		} else if err == os.ErrInvalid {
			w.WriteHeader(422)
			return
		} else if err == errors.ErrNoAvailableActionNode {
			w.WriteHeader(503)
			return
		} else if err == errors.ErrLock {
			w.WriteHeader(523)
			return
		} else if err == errors.ErrZombie {
			w.WriteHeader(524)
			return
		} else {
			w.WriteHeader(500)
		}
		d.logger.Error("Read request is failed", zap.Strings("paths", requestedPaths), zap.Error(err))
		return
	}

	if read.Type() == manager.RT_Folder {
		folder := read.Folder()

		calculateUsageHeader := strings.ToLower(r.Header.Get("X-Calculate-Usage"))
		calculateUsage := len(calculateUsageHeader) > 0 && (strings.Compare(calculateUsageHeader, "1") == 0 || strings.Compare(calculateUsageHeader, "true") == 0)

		if calculateUsage {
			folder.CalculateUsage(func(shadows common.FolderShadows) {
				for _, shadow := range shadows {
					shadow.Size, _ = d.dfs.Size(shadow.Full)
				}
			})
		}

		w.Header().Set("X-Type", "folder")

		if err := json.NewEncoder(w).Encode(folder); err != nil {
			w.WriteHeader(500)
			d.logger.Error(
				"Response of read request is failed",
				zap.Strings("paths", requestedPaths),
				zap.Error(err),
			)
		}
		return
	}

	w.Header().Set("X-Type", "file")

	downloadHeader := strings.ToLower(r.Header.Get("X-Download"))
	download := len(downloadHeader) > 0 && (strings.Compare(downloadHeader, "1") == 0 || strings.Compare(downloadHeader, "true") == 0)

	requestRange := r.Header.Get("Range")
	partialRequest := len(requestRange) > 0

	push, begins, ends := d.prepareResponseHeaders(w, read.File(), download, partialRequest, requestRange)
	if !push {
		return
	}

	if err := read.Read(w, begins, ends); err != nil {
		d.logger.Warn(
			"Streaming file content is failed",
			zap.Strings("paths", requestedPaths),
			zap.Int64("begins", begins),
			zap.Int64("ends", ends),
			zap.Error(err),
		)
	}
}

func (d *dfsRouter) prepareResponseHeaders(w http.ResponseWriter, file *common.File, download bool, partialRequest bool, requestRange string) (bool, int64, int64) {
	w.Header().Set("Content-Type", file.Mime)

	begins, ends := int64(0), int64(file.Size)-1

	if !partialRequest {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", strconv.FormatUint(file.Size, 10))
		if download {
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", file.Name))
		}
		return true, begins, ends
	}

	bytesTag := "bytes="
	bytesIdx := strings.Index(requestRange, bytesTag)
	if bytesIdx == 0 {
		ranges := requestRange[bytesIdx+len(bytesTag):]
		byteRanges := strings.Split(ranges, "-")

		var err error
		begins, err = strconv.ParseInt(byteRanges[0], 10, 64)
		if err != nil {
			w.WriteHeader(416)
			return false, 0, 0
		}
		ends, err = strconv.ParseInt(byteRanges[1], 10, 64)
		if err != nil {
			ends = int64(file.Size) - 1
		}

		if ends > int64(file.Size)-1 || begins > ends {
			w.WriteHeader(416)
			return false, 0, 0
		}

		w.Header().Set("Content-Length", strconv.FormatInt(ends-begins+1, 10))
		w.Header().Set("Content-Encoding", "identity")
		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", begins, ends, file.Size))
		w.WriteHeader(206)
	}

	return true, begins, ends
}
