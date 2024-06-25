package bitcast_go

import (
	"bitcast-go/data"
	"bitcast-go/selferror"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const mergeDirName = "-merge"
const mergeFinishedKey = "merge-finished"

func (db *DB) Merge() error {
	//如果活跃文件是null，则直接返回
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	//如果Merge在进行中，则直接返回
	if db.isMerging {
		db.mu.Unlock()
		return selferror.ErrMergeIsProgress
	}
	//否则，将该标识位置为true
	db.isMerging = true
	defer func() {
		//merge结束之后，需要置为false
		db.isMerging = false
	}()

	//持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		return err
	}
	//将当前活跃文件转化为旧的活跃文件
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	//打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	//记录最近没有参与merge的文件id
	nonMergeId := db.activeFile.FileId

	//取出所有需要merge的文件
	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mu.Unlock()

	//待merge的文件，从小到大进行排序，依次merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	//如果目录存在，说明发生过merge，将其删掉
	if _, err := os.Stat(mergePath); err != nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	//新建一个 merge path 的目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	//打开一个新的临时bitcask实例
	mergeOptions := db.option
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}
	//打开hint文件，存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	//遍历处理每个数据文件
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			//解析实际拿到的key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			//和内存中的索引位置进行比较，如果有效则重写
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				//清除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				//将数据进行重写，通过追加文件的方法
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				//将当前位置索引写到hint文件中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			//增加offset
			offset += size
		}
	}
	//sync 保证持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}
	//写标识merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeId))), //值为最后一个没有参与的文件，即最新的活跃文件,下次打开的时候，如果文件id比其小，则表示都参与过merge
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	err = mergeFinishedFile.Write(encRecord)
	if err != nil {
		return err
	}
	err = mergeFinishedFile.Sync()
	if err != nil {
		return err
	}
	return nil
}

// /tmp/bitcask
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.option.DirPath)) //拿到当前配置目录的父级目录
	base := path.Base(db.option.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}
