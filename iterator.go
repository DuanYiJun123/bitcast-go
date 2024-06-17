package bitcast_go

import (
	"bitcast-go/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Iterator //索引迭代器
	db        *DB
	options   IteratorOptions
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	iterator := db.index.Iterator(opts.Reverse)
	return &Iterator{
		indexIter: iterator,
		db:        db,
		options:   opts,
	}
}

//重新回到迭代器的起点，即第一个数据
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}

//根据传入的key查找到第一个大于（或小于）等于的目标Key,从这个key开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}

//跳转到下一个key
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}

//Valid是否有效，即是否已经遍历完了所有的key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

//当前遍历位置key的数据
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

//当前遍历位置value的数据
func (it *Iterator) Value() ([]byte, error) {
	pos := it.indexIter.Value()
	it.db.mu.Lock()
	defer it.db.mu.RUnlock()
	return it.db.getVauleByPosition(pos)
}

//关闭迭代器，释放资源
func (it *Iterator) Close() {
	it.indexIter.Close()
}

//用于筛选preix不满足条件的key
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		//如果prefix的长度小于等于key的长度，且prefix与key的字节相等，则跳出循环，说明是我们要找的key
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
