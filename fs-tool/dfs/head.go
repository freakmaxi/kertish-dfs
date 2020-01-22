package dfs

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/fs-tool/common"
)

const headEndPoint = "/client/dfs"

var client = http.Client{}

func List(headAddresses []string, source string, usage bool) (*common.Folder, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s%s", headAddresses[0], headEndPoint), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Path", source)
	req.Header.Set("X-CalculateUsage", strconv.FormatBool(usage))

	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}

	switch res.StatusCode {
	case 404:
		return nil, fmt.Errorf("%s is not exists", source)
	case 422:
		return nil, fmt.Errorf("%s should be an absolute path", source)
	case 500:
		return nil, fmt.Errorf("unable to list %s", source)
	}

	var folder *common.Folder
	if err := json.NewDecoder(res.Body).Decode(&folder); err != nil {
		return nil, fmt.Errorf("unable to list %s", source)
	}

	return folder, nil
}

func MakeFolder(headAddresses []string, target string) error {
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s%s", headAddresses[0], headEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-ApplyTo", "folder")
	req.Header.Set("X-Path", target)

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}

	switch res.StatusCode {
	case 409:
		return fmt.Errorf("%s is already exists", target)
	case 422:
		return fmt.Errorf("%s should be an absolute path", target)
	case 500:
		return fmt.Errorf("unable to create %s", target)
	}

	return nil
}

func Change(headAddresses []string, source string, target string, overwrite bool, copy bool) error {
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s%s", headAddresses[0], headEndPoint), nil)
	if err != nil {
		return err
	}
	action := "m"
	if copy {
		action = "c"
	}
	req.Header.Set("X-Path", source)
	req.Header.Set("X-Target", fmt.Sprintf("%s,%s", action, target))
	req.Header.Set("X-Overwrite", strconv.FormatBool(overwrite))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}

	switch res.StatusCode {
	case 404:
		return fmt.Errorf("%s is not exists", source)
	case 409:
		return fmt.Errorf("%s is already exists", target)
	case 422:
		return fmt.Errorf("%s and %s should be absolute paths", source, target)
	case 500:
		if strings.Compare(action, "m") == 0 {
			action = "move"
		} else {
			action = "copy"
		}
		return fmt.Errorf("unable to %s from %s to %s", action, source, target)
	}

	return nil
}

func Delete(headAddresses []string, target string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s%s", headAddresses[0], headEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Path", target)

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}

	switch res.StatusCode {
	case 404:
		return fmt.Errorf("%s is not exists", target)
	case 422:
		return fmt.Errorf("%s should be an absolute path", target)
	case 500:
		return fmt.Errorf("unable to delete from %s", target)
	}

	return nil
}

func Put(headAddresses []string, source string, target string, overwrite bool) error {
	info, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("unable to read %s", source)
	}

	if !info.IsDir() {
		return PutFile(headAddresses, source, target, overwrite)
	}

	return filepath.Walk(source, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if strings.Compare(p, source) == 0 {
			return nil
		}

		parent, child := path.Split(p)
		parent = strings.Replace(parent, source, "", 1)
		targetPath := path.Join(target, parent, child)

		if !info.IsDir() {
			return PutFile(headAddresses, p, targetPath, overwrite)
		}
		if overwrite {
			if err := Delete(headAddresses, targetPath); err != nil {
				return err
			}
		}
		return MakeFolder(headAddresses, targetPath)
	})
}

func PutFile(headAddresses []string, source string, target string, overwrite bool) error {
	contentType, size, err := contentDetails(source)
	if err != nil {
		return fmt.Errorf("unable to read %s", source)
	}

	file, err := os.OpenFile(source, os.O_RDONLY, 0666)
	if err != nil {
		return fmt.Errorf("unable to read %s", source)
	}
	defer file.Close()

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s%s", headAddresses[0], headEndPoint), file)
	if err != nil {
		return err
	}

	req.Header.Set("X-ApplyTo", "file")
	req.Header.Set("X-Path", target)
	req.Header.Set("X-Overwrite", strconv.FormatBool(overwrite))
	req.Header.Set("X-AllowEmpty", strconv.FormatBool(size == 0))
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = size

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}

	switch res.StatusCode {
	case 409:
		return fmt.Errorf("%s is already exists", target)
	case 411:
		return fmt.Errorf("content length should be defined for %s", target)
	case 422:
		return fmt.Errorf("filename must be specified for %s", target)
	case 507:
		return fmt.Errorf("insufficient space")
	case 500:
		return fmt.Errorf("unable to create %s", target)
	}

	return nil
}

func contentDetails(source string) (string, int64, error) {
	file, err := os.OpenFile(source, os.O_RDONLY, 0666)
	if err != nil {
		return "", 0, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return "", 0, err
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return "application/octet-stream", info.Size(), nil
	}

	buf := make([]byte, 512)
	r, err := file.Read(buf)
	if r == 0 || err != nil {
		return "application/octet-stream", info.Size(), nil
	}

	return http.DetectContentType(buf), info.Size(), nil
}

func Pull(headAddresses []string, source string, target string, readRange *common.ReadRange) error {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s%s", headAddresses[0], headEndPoint), nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Path", source)
	if readRange != nil {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", readRange.Begins, readRange.Ends))
	}

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}

	switch res.StatusCode {
	case 404:
		return fmt.Errorf("%s is not exists", source)
	case 422:
		return fmt.Errorf("%s should be an absolute path", source)
	case 500:
		return fmt.Errorf("unable to get %s", source)
	}

	if strings.Compare(res.Header.Get("X-Type"), "file") == 0 {
		file, err := os.OpenFile(target, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return fmt.Errorf("unable to create %s", target)
		}
		defer file.Close()

		if _, err := io.Copy(file, res.Body); err != nil {
			return fmt.Errorf("unsuccessful operation")
		}

		return nil
	}

	if strings.Compare(res.Header.Get("X-Type"), "folder") == 0 && readRange != nil {
		return fmt.Errorf("not possible to use range argument for folders")
	}

	var folder *common.Folder
	if err := json.NewDecoder(res.Body).Decode(&folder); err != nil {
		return fmt.Errorf("unsuccessful operation")
	}

	if err := os.Mkdir(target, 0777); err != nil {
		return fmt.Errorf("unable to create %s", target)
	}

	for _, file := range folder.Files {
		sourcePath := path.Join(source, file.Name)
		targetPath := path.Join(target, file.Name)

		if err := Pull(headAddresses, sourcePath, targetPath, nil); err != nil {
			return fmt.Errorf("unsuccessful operation")
		}
	}

	for _, f := range folder.Folders {
		sourcePath := path.Join(source, f.Name)
		targetPath := path.Join(target, f.Name)

		if err := Pull(headAddresses, sourcePath, targetPath, nil); err != nil {
			return fmt.Errorf("unsuccessful operation")
		}
	}

	return nil
}
