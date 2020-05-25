package common

import (
	"os"
	"strings"
)

type Tree struct {
	folder      *Folder
	folderCache map[string]*FolderShadow
	subs        []*Tree
	subMap      map[string]*Tree
}

func NewTree() *Tree {
	return newTree(nil)
}

func newTree(folder *Folder) *Tree {
	folderCache := make(map[string]*FolderShadow)
	if folder != nil {
		for _, folderShadow := range folder.Folders {
			folderCache[folderShadow.Full] = folderShadow
		}
	}

	return &Tree{
		folder:      folder,
		folderCache: folderCache,
		subs:        make([]*Tree, 0),
		subMap:      make(map[string]*Tree),
	}
}

func (t *Tree) Normalize() []*Folder {
	folders := make([]*Folder, 0)
	t.normalize(t, &folders)

	return folders
}

func (t *Tree) normalize(tree *Tree, folders *[]*Folder) {
	*folders = append(*folders, tree.folder)

	if len(tree.subs) == 0 {
		return
	}

	for _, subTree := range tree.subs {
		t.normalize(subTree, folders)
	}
}

func (t *Tree) Fill(folders []*Folder) error {
	if len(folders) == 0 {
		return nil
	}

	t.folder = folders[0]
	for _, folderShadow := range t.folder.Folders {
		t.folderCache[folderShadow.Full] = folderShadow
	}
	currentTree := t

	for i := 1; i < len(folders); i++ {
		folder := folders[i]

		folderFull := folder.Full
		parentPath, name := Split(folderFull)

		if strings.Compare(currentTree.folder.Full, parentPath) == 0 {
			if _, has := currentTree.folderCache[folderFull]; !has {
				_ = currentTree.folder.NewFolder(name, func(shadow *FolderShadow) error {
					currentTree.folderCache[shadow.Full] = shadow
					return nil
				})
			}

			nt := newTree(folder)
			currentTree.subs = append(currentTree.subs, nt)
			currentTree.subMap[folder.Full] = nt

			continue
		}

		if _, err := t.locate(folder); err != nil { // ErrNotExists all the time
			// Something broken in the tree structure, fill missing parts
			if err := t.fix(currentTree, folder); err != nil {
				return err
			}
		}
		currentTree, _ = t.locate(folder)
	}
	t.ensureStructure(t)

	return nil
}

func (t *Tree) locate(folder *Folder) (*Tree, error) {
	parts := PathTree(folder.Full)

	currentTree := t
	for i := 0; i < len(parts); i++ {
		part := parts[i]

		if strings.Compare(currentTree.folder.Full, part) == 0 {
			continue
		}

		currentTree = t.get(currentTree, parts[i])
		if currentTree == nil {
			return nil, os.ErrNotExist
		}
	}
	return currentTree, nil
}

func (t *Tree) get(searchingTree *Tree, full string) *Tree {
	tree, has := searchingTree.subMap[full]
	if !has {
		return nil
	}
	return tree
}

func (t *Tree) fix(parent *Tree, folder *Folder) error {
	pathTree := PathTree(folder.Full)

	for len(pathTree) > 0 {
		p := pathTree[0]

		if strings.Compare(parent.folder.Full, p) != 0 {
			pathTree = pathTree[1:]
			continue
		}
		pathTree = pathTree[1:]
		break
	}

	if len(pathTree) == 0 {
		return t.fix(t, folder)
	}

	tree := parent
	for i := 0; i < len(pathTree); i++ {
		p := pathTree[i]

		childTree := t.get(tree, p)
		if childTree != nil {
			tree = childTree
			continue
		}

		_, name := Split(p)

		if err := tree.folder.NewFolder(name, func(shadow *FolderShadow) error {
			tree.folderCache[shadow.Full] = shadow
			return nil
		}); err != nil && err != os.ErrExist {
			return err
		}

		if strings.Compare(folder.Full, p) == 0 {
			childTree = newTree(folder)
		} else {
			childTree = newTree(NewFolder(p))
		}

		tree.subs = append(tree.subs, childTree)
		tree.subMap[childTree.folder.Full] = childTree

		tree = childTree
	}

	return nil
}

func (t *Tree) ensureStructure(tree *Tree) {
	for _, folderShadow := range tree.folder.Folders {
		if _, has := tree.subMap[folderShadow.Full]; !has {
			nt := newTree(NewFolder(folderShadow.Full))
			tree.subs = append(tree.subs, nt)
			tree.subMap[folderShadow.Full] = nt
		}
	}

	for _, treeItem := range tree.subs {
		t.ensureStructure(treeItem)
	}
}
