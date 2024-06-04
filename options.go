package bitcast_go

import "os"

type Options struct {
	DirPath string //数据库数据目录

	//数据文件的大小阈值
	DataFileSize int64

	//每次写入数据是否持久化
	SyncWrites bool

	//索引类型
	IndexerType IndexerType
}

type IndexerType = int8

const (
	//BTree索引
	BTree IndexerType = iota + 1

	//ART 自适应基数树索引
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexerType:  BTree,
}
