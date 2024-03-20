package fio

import "os"

//FileIO标准系统文件
type FileIO struct {
	fd *os.File
}

//初始化标准文件IO
func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm) //打开文件，如果不存在则创建，并赋予读写权限，和append only
	if err != nil {
		return nil, err
	}
	return &FileIO{fd}, nil
}

//从文件的给定位置读取对应的数据
func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

//写入字节数组到文件中
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

//持久化数据
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

//关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
