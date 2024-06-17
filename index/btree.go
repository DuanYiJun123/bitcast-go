package index

import (
	"bitcast-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// Btree index的一种实现：BTree索引，主要封装Google btree的库
type Btree struct {
	//在并发写操作时，可能不安全，所以需要一个加锁保护
	tree *btree.BTree
	//写互斥
	lock *sync.RWMutex
}

// NewBTree NewBTree初始化BTree索引结构
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

//BTree 索引迭代器
type btreeIterator struct {
	currIndex int     //当前遍历的下标位置
	reverse   bool    //是否是反向遍历
	values    []*Item //key+位置索引信息
}

//newBTreeIterator
func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	//将所有的数据存放在数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}

	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

//重新回到迭代器的起点，即第一个数据
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

//根据传入的key查找到第一个大于（或小于）等于的目标Key,从这个key开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		//二分查找
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

//跳转到下一个key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

//Valid是否有效，即是否已经遍历完了所有的key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

//当前遍历位置key的数据
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

//当前遍历位置value的数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

//关闭迭代器，释放资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}

func (bt *Btree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}
