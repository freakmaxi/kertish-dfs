package common

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
)

const pathSeparator = "/"

func EmptyChecksum() string {
	return hex.EncodeToString(sha512.New512_256().Sum(nil))
}

func CorrectPaths(paths []string) []string {
	for i := range paths {
		folderPath := paths[i]

		if strings.Index(folderPath, pathSeparator) != 0 {
			folderPath = fmt.Sprintf("%s%s", pathSeparator, folderPath)
		}
		folderPath = path.Clean(folderPath)

		if len(folderPath) == 0 {
			folderPath = pathSeparator
		}

		paths[i] = folderPath
	}
	return paths
}

func CorrectPath(folderPath string) string {
	return CorrectPaths([]string{folderPath})[0]
}

func PathTree(rootPath *string, folderPath string) []string {
	folderPath = CorrectPath(folderPath)
	if strings.Compare(folderPath, pathSeparator) == 0 {
		return []string{pathSeparator}
	}

	folderTree := make([]string, 0)
	split := strings.Split(folderPath, pathSeparator)
	for len(split) > 0 {
		p := strings.Join(split, pathSeparator)
		if rootPath == nil ||
			strings.Compare(*rootPath, pathSeparator) == 0 ||
			strings.HasPrefix(p, *rootPath) {
			folderTree = append([]string{CorrectPath(p)}, folderTree...)
		}

		split = split[:len(split)-1]
	}

	return folderTree
}

func Split(path string) (string, string) {
	path = CorrectPath(path)
	if strings.Compare(path, pathSeparator) == 0 {
		return pathSeparator, ""
	}

	idx := strings.LastIndex(path, pathSeparator)
	if idx == 0 {
		return pathSeparator, path[1:]
	}

	return path[:idx], path[idx+1:]
}

func Join(inputs ...string) string {
	for i, p := range inputs {
		if strings.Index(p, pathSeparator) == 0 {
			p = p[1:]
		}
		inputs[i] = p
	}
	return CorrectPath(strings.Join(inputs, pathSeparator))
}

func DivideParts(folderPath string) []string {
	return strings.Split(folderPath, pathSeparator)
}

func Absolute(basePath string, folderPath string) string {
	if strings.Index(folderPath, pathSeparator) == 0 {
		basePath = pathSeparator
	}

	targetParts := DivideParts(folderPath)
	for len(targetParts) > 0 {
		if strings.Compare(targetParts[0], "..") != 0 {
			basePath = Join(basePath, targetParts[0])
			targetParts = targetParts[1:]
			continue
		}
		parentPath, _ := Split(basePath)
		basePath = parentPath
		targetParts = targetParts[1:]
	}
	return basePath
}

func ValidatePath(folderPath string) bool {
	return strings.Index(folderPath, pathSeparator) == 0
}
