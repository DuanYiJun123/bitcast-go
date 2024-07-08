package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

//io 内存文件映射,这里仅仅用来读数据，用来加速启动
type MMap struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIoManager(fileName string) (*MMap, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

//写入字节数组到文件中
func (mmap *MMap) Write([]byte) (int, error) {
	panic("not implemented")
}

//持久化数据
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

//关闭文件
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

//获取到对应文件大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
