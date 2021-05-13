package common

import "time"

// TreeShadow holds the vital folder details to create the tree output
type TreeShadow struct {
	Full     string      `json:"full"`
	Name     string      `json:"name"`
	Created  time.Time   `json:"created"`
	Modified time.Time   `json:"modified"`
	Size     uint64      `json:"size"`
	Folders  TreeShadows `json:"folders"`
}

// TreeShadows is the definition of the pointer array of TreeShadow struct
type TreeShadows []*TreeShadow

// NewTreeShadowFromTree creates a TreeShadow struct using the tree details.
func NewTreeShadowFromTree(tree *Tree) *TreeShadow {
	subShadows := make(TreeShadows, 0)

	for _, subTree := range tree.subs {
		subShadows = append(subShadows, NewTreeShadowFromTree(subTree))
	}

	return &TreeShadow{
		Full:     tree.folder.Full,
		Name:     tree.folder.Name,
		Created:  tree.folder.Created,
		Modified: tree.folder.Modified,
		Size:     tree.folder.Size,
		Folders:  subShadows,
	}
}
