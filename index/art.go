package index

import (
	"bitcast-go/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

//自适应基数树索引
//主要封装了 https://github.com/plar/go-adaptive-radix-tree库
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewArt() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

//向索引中存储key 对应的数据位置信息
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	art.tree.Insert(key, pos)
	art.lock.Unlock()
	return true
}

//根据key 取出对应的索引位置信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

//根据Key 删除对应的位置信息
func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	_, deleted := art.tree.Delete(key)
	art.lock.Unlock()
	return deleted
}

//索引中存在的数据量
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.Unlock()
	return size
}

//Art 索引迭代器
type artIterator struct {
	currIndex int     //当前遍历的下标位置
	reverse   bool    //是否是反向遍历
	values    []*Item //key+位置索引信息
}

//newArtIterator
func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int

	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues) //把saveValues传入，即可执行该函数，values就会放满数据

	return &artIterator{
		currIndex: 0,
		reverse:   reverse,
		values:    values,
	}
}

//重新回到迭代器的起点，即第一个数据
func (at *artIterator) Rewind() {
	at.currIndex = 0
}

//根据传入的key查找到第一个大于（或小于）等于的目标Key,从这个key开始遍历
func (at *artIterator) Seek(key []byte) {
	if at.reverse {
		at.currIndex = sort.Search(len(at.values), func(i int) bool {
			return bytes.Compare(at.values[i].key, key) <= 0
		})
	} else {
		//二分查找
		at.currIndex = sort.Search(len(at.values), func(i int) bool {
			return bytes.Compare(at.values[i].key, key) >= 0
		})
	}
}

//跳转到下一个key
func (at *artIterator) Next() {
	at.currIndex += 1
}

//Valid是否有效，即是否已经遍历完了所有的key，用于退出遍历
func (at *artIterator) Valid() bool {
	return at.currIndex < len(at.values)
}

//当前遍历位置key的数据
func (at *artIterator) Key() []byte {
	return at.values[at.currIndex].key
}

//当前遍历位置value的数据
func (at *artIterator) Value() *data.LogRecordPos {
	return at.values[at.currIndex].pos
}

//关闭迭代器，释放资源
func (at *artIterator) Close() {
	at.values = nil
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art.tree == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}
