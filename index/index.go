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
}

type IndexType = int8

const (
	// Btrees 索引
	Btrees IndexType = iota + 1

	// ART 自适应基数树索引
	ART
)

// NewIndexer 根据类型初始化索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btrees:
		return NewBTree()
	case ART:
		//todo
		return nil
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
