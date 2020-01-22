package routing

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/head-node/src/common"
)

func (d *dfsRouter) handleGet(w http.ResponseWriter, r *http.Request) {
	requestedPath := r.Header.Get("X-Path")

	if !common.ValidatePath(requestedPath) {
		w.WriteHeader(422)
		return
	}

	calculateUsageHeader := strings.ToLower(r.Header.Get("X-CalculateUsage"))
	calculateUsage := len(calculateUsageHeader) > 0 && (strings.Compare(calculateUsageHeader, "1") == 0 || strings.Compare(calculateUsageHeader, "true") == 0)

	downloadHeader := strings.ToLower(r.Header.Get("X-Download"))
	download := len(downloadHeader) > 0 && (strings.Compare(downloadHeader, "1") == 0 || strings.Compare(downloadHeader, "true") == 0)

	requestRange := r.Header.Get("Range")
	partialRequest := len(requestRange) > 0

	if err := d.dfs.Read(
		requestedPath,
		func(folder *common.Folder) error {
			w.Header().Set("X-Type", "folder")

			if calculateUsage {
				folder.CalculateUsage(func(shadows common.FolderShadows) {
					for _, folder := range shadows {
						size, _ := d.dfs.Size(folder.Full)
						folder.Size = size
					}
				})
			}

			return json.NewEncoder(w).Encode(folder)
		},
		func(file *common.File, streamHandler func(writer io.Writer, begins int64, ends int64) error) error {
			w.Header().Set("X-Type", "file")
			push, begins, ends := d.prepareResponseHeaders(w, file, download, partialRequest, requestRange)
			if !push {
				return nil
			}
			return streamHandler(w, begins, ends)
		}); err != nil {
		if err == os.ErrNotExist {
			w.WriteHeader(404)
			return
		} else {
			w.WriteHeader(500)
		}
		fmt.Printf("ERROR: Get request for %s is failed. %s\n", requestedPath, err.Error())
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
