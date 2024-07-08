package fio

const DataFilePerm = 0644 //权限常量

type FileIOType = byte

const (
	StandardFio FileIOType = iota
	MemoryMap
)

//抽象IO管理接口，可以接入不同的IO类型，目前支持标准文件IO
type IOManager interface {
	//从文件的给定位置读取对应的数据
	Read([]byte, int64) (int, error)

	//写入字节数组到文件中
	Write([]byte) (int, error)

	//持久化数据
	Sync() error

	//关闭文件
	Close() error

	//获取到对应文件大小
	Size() (int64, error)
}

// NewIoManager 初始化IOManager 目前只支持FileIO
func NewIoManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFio:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIoManager(fileName)
	default:
		panic("unsupported io type")
	}
	return NewFileIOManager(fileName)
}
