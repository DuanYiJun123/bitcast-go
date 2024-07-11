package index

import (
	"bitcast-go/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

//B+树索引 主要封装了go.etcd.io/bbolt库

type BPlusTree struct {
	tree *bbolt.DB //支持并发访问，无需再加锁了
}

func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opts := bbolt.DefaultOptions
	opts.NoSync = !syncWrites

	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opts)
	if err != nil {
		panic("failed to open bptree")
	}

	//创建对应的Bucket
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("faild to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

//向索引中存储key 对应的数据位置信息
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldValue = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value in bptree")
	}
	if len(oldValue) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldValue)
}

//根据key 取出对应的索引位置信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return pos
}

//根据Key 删除对应的位置信息
func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if value := bucket.Get(key); len(value) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to put value in bptree")
	}
	if len(oldValue) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldValue), true
}

//索引中存在的数据量
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}
	return size
}

//
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

//B+树迭代器
type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bi.Rewind()
	return bi
}

func (bi *bptreeIterator) Rewind() {
	if bi.reverse {
		bi.currKey, bi.currValue = bi.cursor.Last()
	} else {
		bi.currKey, bi.currValue = bi.cursor.First()
	}

}

//根据传入的key查找到第一个大于（或小于）等于的目标Key,从这个key开始遍历
func (bi *bptreeIterator) Seek(key []byte) {
	bi.cursor.Seek(key)
}

//跳转到下一个key
func (bi *bptreeIterator) Next() {
	if bi.reverse {
		bi.currKey, bi.currValue = bi.cursor.Prev()
	} else {
		bi.currKey, bi.currValue = bi.cursor.Next()
	}
}

//Valid是否有效，即是否已经遍历完了所有的key，用于退出遍历
func (bi *bptreeIterator) Valid() bool {
	return len(bi.currKey) != 0
}

//当前遍历位置key的数据
func (bi *bptreeIterator) Key() []byte {
	return bi.currKey
}

//当前遍历位置value的数据
func (bi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bi.currValue)
}

//关闭迭代器，释放资源
func (bi *bptreeIterator) Close() {
	_ = bi.tx.Rollback()
}
