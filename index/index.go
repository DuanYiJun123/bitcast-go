package index

import (
	"bitcast-go/data"
	"bytes"
	"github.com/google/btree"
)

type indexer interface {
	//向索引中存储key 对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	//根据key 取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	//根据Key 删除对应的位置信息
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
