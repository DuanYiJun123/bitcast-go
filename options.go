package bitcast_go

import "os"

type Options struct {
	DirPath string //数据库数据目录

	//数据文件的大小阈值
	DataFileSize int64

	//每次写入数据是否持久化
	SyncWrites bool

	//累计写到多少字节进行持久化
	BytesPerSync uint

	//索引类型
	IndexerType IndexerType

	//启动时是否需要使用mmap内存映射
	MMapAtStartup bool

	//数据文件合并的阈值
	DataFileMergeRatio float32
}

type IndexerType = int8

const (
	//BTree索引
	BTree IndexerType = iota + 1

	//ART 自适应基数树索引
	ART

	//B+树索引
	BPlusTree
)

//索引迭代器配置项
type IteratorOptions struct {
	//遍历前缀为指定值的key，默认为空
	Prefix []byte
	//是否反向遍历，默认false为正向
	Reverse bool
}

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024,
	SyncWrites:         false,
	BytesPerSync:       0,
	IndexerType:        BTree,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	//一个批次中最大的数据量
	MaxBatchNum uint
	//提交时，是否sync持久化
	SyncWrites bool
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
