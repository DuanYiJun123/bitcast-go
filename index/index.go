package index

import (
	"bitcast-go/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	//向索引中存储key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	//根据key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	//根据Key 删除对应的位置信息
	Delete(key []byte) bool

	//索引中存在的数据量
	Size() int
	//
	Iterator(reverse bool) Iterator
}

type IndexType = int8

const (
	// Btrees 索引
	Btrees IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

// NewIndexer 根据类型初始化索引
func NewIndexer(typ IndexType, dirPath string) Indexer {
	switch typ {
	case Btrees:
		return NewBTree()
	case ART:
		return NewArt()
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

//通用的索引迭代器
type Iterator interface {
	//重新回到迭代器的起点，即第一个数据
	Rewind()
	//根据传入的key查找到第一个大于（或小于）等于的目标Key,从这个key开始遍历
	Seek(key []byte)
	//跳转到下一个key
	Next()
	//Valid是否有效，即是否已经遍历完了所有的key，用于退出遍历
	Valid() bool
	//当前遍历位置key的数据
	Key() []byte
	//当前遍历位置value的数据
	Value() *data.LogRecordPos
	//关闭迭代器，释放资源
	Close()
}
