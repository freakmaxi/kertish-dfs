package common

import (
	"crypto/md5"
	"encoding/hex"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/freakmaxi/kertish-dfs/basics/errors"
	"github.com/freakmaxi/kertish-dfs/basics/hooks"
)

// Folder struct is to hold the virtual folder details associated in dfs cluster
type Folder struct {
	Full     string        `json:"full"`
	Name     string        `json:"name"`
	Created  time.Time     `json:"created"`
	Modified time.Time     `json:"modified"`
	Size     uint64        `json:"size" bson:"-"`
	Folders  FolderShadows `json:"folders"`
	Files    Files         `json:"files"`
	Hooks    hooks.Hooks   `json:"hooks,omitempty"`
}

// NewFolder creates a new empty Folder struct with folderPath
func NewFolder(folderPath string) *Folder {
	folderPath = CorrectPath(folderPath)
	_, name := Split(folderPath)

	return &Folder{
		Full:     folderPath,
		Name:     name,
		Created:  time.Now().UTC(),
		Modified: time.Now().UTC(),
		Folders:  make(FolderShadows, 0),
		Files:    make(Files, 0),
		Hooks:    make(hooks.Hooks, 0),
	}
}

// NewFolder creates an empty sub Folder in the current Folder
func (f *Folder) NewFolder(name string) (*Folder, error) {
	name = CorrectPath(name)
	name = name[1:]

	if len(name) == 0 {
		return nil, os.ErrInvalid
	}

	if strings.Contains(name, pathSeparator) {
		return nil, os.ErrInvalid
	}

	if f.exists(name) {
		return nil, os.ErrExist
	}

	folderShadow := NewFolderShadow(Join(f.Full, name))

	f.Folders = append(f.Folders, folderShadow)
	sort.Sort(f.Folders)
	f.Modified = time.Now().UTC()

	return NewFolder(Join(f.Full, name)), nil
}

// NewFile creates a File struct that points the actual file in the current folder
func (f *Folder) NewFile(name string) (*File, error) {
	name = CorrectPath(name)
	name = name[1:]

	if len(name) == 0 {
		return nil, os.ErrInvalid
	}

	if strings.Contains(name, pathSeparator) {
		return nil, os.ErrInvalid
	}

	if f.exists(name) {
		return nil, os.ErrExist
	}

	nf := newFile(name)
	f.Files = append(f.Files, nf)
	sort.Sort(f.Files)
	f.Modified = time.Now().UTC()

	return nf, nil
}

// CreateJoinedFolder merges the provided Folder array as a Folder
func CreateJoinedFolder(folders []*Folder) (*Folder, error) {
	hash := md5.New()

	joinedFolder := NewFolder("/TEMP")
	for _, f := range folders {
		if _, err := hash.Write([]byte(f.Full)); err != nil {
			return nil, err
		}

		for _, fs := range f.Folders {
			shadow := *fs

			fp := joinedFolder.Folder(shadow.Name)
			if fp != nil {
				continue
			}
			joinedFolder.Folders = append(joinedFolder.Folders, &shadow)
		}

		for _, file := range f.Files {
			shadow := *file

			fileCheck := joinedFolder.File(shadow.Name)
			if fileCheck != nil {
				return nil, errors.ErrJoinConflict
			}
			joinedFolder.Files = append(joinedFolder.Files, &shadow)
		}
	}
	joinedFolder.Name = hex.EncodeToString(hash.Sum(nil))
	joinedFolder.Full = Join(pathSeparator, joinedFolder.Name)

	return joinedFolder, nil
}

// Folder searches the Folder by name and return the full folderPath
func (f *Folder) Folder(name string) *string {
	for _, fs := range f.Folders {
		if strings.Compare(fs.Name, name) == 0 {
			folderFull := Join(f.Full, name)
			return &folderFull
		}
	}
	return nil
}

// File searches the File by name and returns the File struct that points the file in dfs cluster
func (f *Folder) File(name string) *File {
	for _, sf := range f.Files {
		if strings.Compare(sf.Name, name) == 0 {
			return sf
		}
	}
	return nil
}

// ReplaceFile searches the File by name and replace it with the provided File struct
// If File has been found and the File struct is null, it will remove the File from the Folder
// If File hasn't been found and File struct is not null, it will add the File to the Folder
func (f *Folder) ReplaceFile(name string, file *File) {
	for i, sf := range f.Files {
		if strings.Compare(sf.Name, name) == 0 {
			if file == nil {
				f.Files = append(f.Files[:i], f.Files[i+1:]...)
			} else {
				f.Files[i] = file
			}
			sort.Sort(f.Files)
			f.Modified = time.Now().UTC()
			return
		}
	}

	if file == nil {
		return
	}

	f.Files = append(f.Files, file)
	sort.Sort(f.Files)
	f.Modified = time.Now().UTC()
}

// DeleteFolder searches the Folder by name and take action base on the result of deleteFolderHandler
// if deleteFolderHandler raises error, deletion will be canceled
// if Folder isn't being found, it will return ErrNotExists error
func (f *Folder) DeleteFolder(name string, deleteFolderHandler func(string) error) error {
	for i, fs := range f.Folders {
		if strings.Compare(fs.Name, name) == 0 {
			if err := deleteFolderHandler(Join(f.Full, name)); err != nil {
				return err
			}
			f.Folders = append(f.Folders[:i], f.Folders[i+1:]...)
			sort.Sort(f.Folders)
			f.Modified = time.Now().UTC()
			return nil
		}
	}
	return os.ErrNotExist
}

// DeleteFile searches the File by name and take action base on the result of deleteFileHandler
// if deleteFileHandler raises error, deletion will be canceled
// if File isn't being found, it will return ErrNotExists error
func (f *Folder) DeleteFile(name string, deleteFileHandler func(*File) error) error {
	for i, sf := range f.Files {
		if strings.Compare(sf.Name, name) == 0 {
			if err := deleteFileHandler(sf); err != nil {
				return err
			}
			f.Files = append(f.Files[:i], f.Files[i+1:]...)
			sort.Sort(f.Files)
			f.Modified = time.Now().UTC()
			return nil
		}
	}
	return os.ErrNotExist
}

// CalculateUsage calculates the total size of the folder including the sub folders recursively
// calculateUsageHandler parameter is mandatory and should fill the FolderShadows size fields to have the
// correct size calculation of the Folder size
func (f *Folder) CalculateUsage(calculateUsageHandler func(FolderShadows)) {
	s := uint64(0)

	for _, file := range f.Files {
		s += file.Size
	}

	if len(f.Folders) > 0 {
		calculateUsageHandler(f.Folders)
		for _, folder := range f.Folders {
			s += folder.Size
		}
	}

	f.Size = s
}

// CloneInto copies the details of the current Folder to the provided target Folder
func (f *Folder) CloneInto(target *Folder) {
	if target == nil {
		return
	}

	target.Folders = make(FolderShadows, 0)
	for _, f := range f.Folders {
		shadow := *f
		target.Folders = append(target.Folders, &shadow)
	}

	target.Files = make(Files, 0)
	for _, f := range f.Files {
		shadow := *f
		target.Files = append(target.Files, &shadow)
	}
}

// Locked checks if the folder has any locked File
func (f *Folder) Locked() bool {
	for _, file := range f.Files {
		if file.Locked() {
			return true
		}
	}
	return false
}

func (f *Folder) exists(name string) bool {
	file := f.File(name)
	if file != nil {
		return true
	}

	return f.Folder(name) != nil
}
