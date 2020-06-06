package manager

import (
	"io"

	"github.com/freakmaxi/kertish-dfs/basics/common"
)

type ReadType int

const (
	RT_Folder ReadType = 1
	RT_File   ReadType = 2
)

type Read interface {
	Type() ReadType

	Folder() *common.Folder
	File() *common.File

	Read(w io.Writer, begins int64, ends int64) error
}

type read struct {
	folder *common.Folder

	file          *common.File
	streamHandler func(w io.Writer, begins int64, ends int64) error
}

func newReadForFolder(folder *common.Folder) Read {
	return &read{
		folder: folder,
	}
}

//
//streamHandler func(writer io.Writer, begins int64, ends int64) error
func newReadForFile(file *common.File, streamHandler func(w io.Writer, begins int64, ends int64) error) Read {
	return &read{
		file:          file,
		streamHandler: streamHandler,
	}
}

func (r *read) Type() ReadType {
	if r.file != nil {
		return RT_File
	}
	return RT_Folder
}

func (r *read) Folder() *common.Folder {
	return r.folder
}

func (r *read) File() *common.File {
	return r.file
}

func (r *read) Read(w io.Writer, begins int64, ends int64) error {
	return r.streamHandler(w, begins, ends)
}

var _ Read = &read{}
