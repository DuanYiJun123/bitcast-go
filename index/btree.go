package index

import (
	"bitcast-go/data"
	"github.com/google/btree"
	"sync"
)

//BTree索引，主要封装Google btree的库
type Btree struct {
	//在并发写操作时，可能不安全，所以需要一个加锁保护
	tree *btree.BTree
	//写互斥
	lock *sync.RWMutex
}

//NewBTree初始化BTree索引结构
func NewBTree() *Btree {
	return &Btree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{
		key: key,
		pos: pos,
	}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{
		key: key,
	}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *Btree) Delete(key []byte) bool {
	it := &Item{
		key: key,
	}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}
