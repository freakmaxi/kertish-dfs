package dfs

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/common"
)

const headEndPoint = "/client/dfs"

var client = http.Client{}

func List(headAddresses []string, source string, usage bool) (*common.Folder, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s%s", headAddresses[0], headEndPoint), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Path", createXPath([]string{source}))
	req.Header.Set("X-Calculate-Usage", strconv.FormatBool(usage))

	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}
	defer res.Body.Close()

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
	req.Header.Set("X-Apply-To", "folder")
	req.Header.Set("X-Path", createXPath([]string{target}))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}
	defer res.Body.Close()

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

func Change(headAddresses []string, sources []string, target string, overwrite bool, copy bool) error {
	req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s%s", headAddresses[0], headEndPoint), nil)
	if err != nil {
		return err
	}
	action := "m"
	if copy {
		action = "c"
	}
	req.Header.Set("X-Path", createXPath(sources))
	req.Header.Set("X-Target", fmt.Sprintf("%s,%s", action, url.QueryEscape(target)))
	req.Header.Set("X-Overwrite", strconv.FormatBool(overwrite))

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case 404:
		return fmt.Errorf("%s is/are not exists", sourcesErrorString(sources))
	case 409:
		return fmt.Errorf("%s is already exists", target)
	case 412:
		return fmt.Errorf("%s have conflicts between file(s)/folder(s)", sourcesErrorString(sources))
	case 422:
		return fmt.Errorf("%s and %s should be full and absolute paths", sourcesErrorString(sources), target)
	case 524:
		return fmt.Errorf("%s is zombie or has zombie", sourcesErrorString(sources))
	case 500:
		if strings.Compare(action, "m") == 0 {
			action = "move"
		} else {
			action = "copy"
		}
		return fmt.Errorf("unable to %s from %s to %s", action, sourcesErrorString(sources), target)
	}

	return nil
}

func Delete(headAddresses []string, target string, killZombies bool) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s%s", headAddresses[0], headEndPoint), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Path", createXPath([]string{target}))

	if killZombies {
		req.Header.Set("X-Kill-Zombies", "true")
	}

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case 404:
		return fmt.Errorf("%s is not exists", target)
	case 422:
		return fmt.Errorf("%s should be an absolute path", target)
	case 500:
		return fmt.Errorf("unable to delete from %s", target)
	case 523:
		return fmt.Errorf("%s is locked or has locked file(s)", target)
	case 524:
		return fmt.Errorf("%s is zombie or has zombie", target)
	case 525:
		return fmt.Errorf("%s is/has still alive zombie, try again to kill", target)
	case 526:
		return fmt.Errorf("inconsistency detected, repair is required to fix the problem")
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
			if err := Delete(headAddresses, targetPath, false); err != nil {
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

	req.Header.Set("X-Apply-To", "file")
	req.Header.Set("X-Path", createXPath([]string{target}))
	req.Header.Set("X-Overwrite", strconv.FormatBool(overwrite))
	req.Header.Set("X-Allow-Empty", strconv.FormatBool(size == 0))
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = size

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}
	defer res.Body.Close()

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

func Pull(headAddresses []string, sources []string, target string, readRange *common.ReadRange) error {
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s%s", headAddresses[0], headEndPoint), nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Path", createXPath(sources))
	if readRange != nil {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", readRange.Begins, readRange.Ends))
	}

	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%s: head node is not reachable", headAddresses[0])
	}
	defer res.Body.Close()

	isFile := strings.Compare(res.Header.Get("X-Type"), "file") == 0

	switch res.StatusCode {
	case 404:
		return fmt.Errorf("%s is/are not exists", sourcesErrorString(sources))
	case 422:
		if strings.Compare(res.Header.Get("X-Type"), "file") == 0 {
			return fmt.Errorf("%s should be absolute path(s)", sourcesErrorString(sources))
		}
		return fmt.Errorf("combining dfs folder(s) to local folder is not possible")
	case 500:
		return fmt.Errorf("unable to get %s", sourcesErrorString(sources))
	case 523:
		return fmt.Errorf("%s is locked or has locked file(s)", sourcesErrorString(sources))
	case 524:
		return fmt.Errorf("%s is zombie or has zombie", sourcesErrorString(sources))
	}

	if isFile {
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

	if err := os.MkdirAll(target, 0777); err != nil {
		return fmt.Errorf("unable to create %s", target)
	}

	for _, file := range folder.Files {
		sourcePath := path.Join(sources[0], file.Name)
		targetPath := path.Join(target, file.Name)

		if err := Pull(headAddresses, []string{sourcePath}, targetPath, nil); err != nil {
			return fmt.Errorf("unsuccessful operation")
		}
	}

	for _, f := range folder.Folders {
		sourcePath := path.Join(sources[0], f.Name)
		targetPath := path.Join(target, f.Name)

		if err := Pull(headAddresses, []string{sourcePath}, targetPath, nil); err != nil {
			return fmt.Errorf("unsuccessful operation")
		}
	}

	return nil
}

func createXPath(sources []string) string {
	if len(sources) == 1 {
		return url.QueryEscape(sources[0])
	}
	for i := range sources {
		sources[i] = url.QueryEscape(sources[i])
	}
	return fmt.Sprintf("j,%s", strings.Join(sources, ","))
}

func sourcesErrorString(sources []string) string {
	for i := range sources {
		sources[i], _ = url.QueryUnescape(sources[i])
	}
	return strings.Join(sources, " + ")
}
