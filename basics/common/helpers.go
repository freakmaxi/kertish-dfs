package common

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"path"
	"strings"
)

const pathSeparator = "/"

// EmptyChecksum checks if the checksum is empty
func EmptyChecksum() string {
	return hex.EncodeToString(sha512.New512_256().Sum(nil))
}

// CorrectPaths fix multiple paths to the correct format base on dfs requirement
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

// CorrectPath fixes the path to the correct format base on dfs requirement
func CorrectPath(folderPath string) string {
	return CorrectPaths([]string{folderPath})[0]
}

// PathTree creates a path list from rootPath to folderPath
// every entry will contain the full path of path pointed in the tree
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

// Split just splits the path to parent path and path name in the way of dfs required
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

// Join joins the paths with suitable path separator and creates a full path
func Join(inputs ...string) string {
	for i, p := range inputs {
		if strings.Index(p, pathSeparator) == 0 {
			p = p[1:]
		}
		inputs[i] = p
	}
	return CorrectPath(strings.Join(inputs, pathSeparator))
}

// DivideParts splits the full path to its path name counter parts
func DivideParts(folderPath string) []string {
	return strings.Split(folderPath, pathSeparator)
}

// Absolute creates the absolute path definition base on basePath
// it accepts relative paths in folderPath
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

// ValidatePath checks if the path is a valid path for dfs
func ValidatePath(folderPath string) bool {
	return strings.Index(folderPath, pathSeparator) == 0
}
