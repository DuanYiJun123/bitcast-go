package bitcast_go

import (
	"bitcast-go/data"
	"bitcast-go/fio"
	"bitcast-go/index"
	"bitcast-go/selferror"
	"bitcast-go/utils"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const seqNoKey = "seq.no"
const fileLockName = "flock"

// DB bitcast存储引擎实例
type DB struct {
	option          Options                   //配置信息
	mu              *sync.RWMutex             //锁
	fileIds         []int                     //仅用于加载索引的时候使用（因为在加载磁盘文件的时候，已经将文件Id取出，但是在olderFiles的map里面是无序的，所以这里需要复用一下这个ids）
	activeFile      *data.DataFile            //当前活跃数据文件，可以用于写入
	olderFiles      map[uint32]*data.DataFile //旧的数据文件，只能用于读
	index           index.Indexer             //内存索引
	seqNo           uint64                    //事务序列号，全局递增
	isMerging       bool                      //是否正在Merge
	seqNoFileExists bool                      //存储事务序列号文件是否存在
	isInitial       bool                      //是否是第一次初始化此数据目录
	fileLock        *flock.Flock              //文件锁，保证多进程之间互斥
	bytesWrite      uint                      //当前写了多少字节的累计值
	reclaimSize     int64                     //表示有多少数据是无效的
}

// 存储引擎统计信息
type Stat struct {
	KeyNum          uint  //key的总数量
	DataFileNum     uint  //数据文件的数量
	ReclaimableSize int64 //可以进行merge回收的数据量，以字节为单位
	DiskSize        int64 //数据目录所占磁盘空间的大小
}

// 返回数据库的相关统计信息
func (db *DB) Stat() *Stat {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var dataFiles = uint(len(db.olderFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}
	size, err := utils.DirSize(db.option.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size:%v", err))
	}
	return &Stat{
		KeyNum:          uint(db.index.Size()),
		DataFileNum:     dataFiles,
		ReclaimableSize: db.reclaimSize,
		DiskSize:        size,
	}
}

// Open 打开bitcask存储引擎实例
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool

	//判断数据目录是否存在，如果不存在的话，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName)) //拿到文件锁
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, selferror.ErrDatabaseIsUsing
	}

	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	//初始化DB实例结构体
	db := &DB{
		option:     options,
		mu:         new(sync.RWMutex),
		activeFile: nil,
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexerType, options.DirPath, options.SyncWrites),
		isInitial:  isInitial,
		fileLock:   fileLock,
	}

	//加载merge目录
	err = db.loadMergeFiles()
	if err != nil {
		return nil, err
	}

	//加载数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//B+树索引不需要从数据文件中加载索引
	if options.IndexerType != BPlusTree {
		//从Hint索引文件中加载索引
		err = db.loadIndexFromHintFile()
		if err != nil {
			return nil, err
		}

		//从数据文件中加载索引
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
		//重置IO类型为标准文件IO
		if db.option.MMapAtStartup {
			err := db.resetIoType()
			if err != nil {
				return nil, err
			}
		}
	}

	//如果是B+树，取出当前事务序列号
	if options.IndexerType == BPlusTree {
		err := db.loadSeqNo()
		if err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return errors.New("invalid merge ratio,must between 0 and 1")
	}
	return nil
}

func (db *DB) BackUp(dir string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	return utils.CopyDir(db.option.DirPath, dir, []string{fileLockName})
}

// 写入Key/Value 数据 key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	//判断key 是否有效
	if len(key) == 0 {
		return selferror.ErrKeyIsEmpty
	}

	//构造LogRecord结构体
	log_record := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecordWithLock(log_record)
	if err != nil {
		return err
	}
	//更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		db.reclaimSize += int64(pos.Size)
		return selferror.ErrIndexUpdateFailed
	}
	return nil
}

// Delete 根据Key删除对应的数据
func (db *DB) Delete(key []byte) error {
	//判断key的有效性
	if len(key) == 0 {
		return selferror.ErrKeyIsEmpty
	}

	//先检查key是否存在，如果不存在的话直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	//构造logRecord，标识其是被删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}
	db.reclaimSize += int64(pos.Size) //本身这条数据也是可以merge清理的，所以这里可以直接添加

	//从内存索引当中将对应的key删除
	pos, ok := db.index.Delete(key)
	if !ok {
		return selferror.ErrIndexUpdateFailed
	}
	if pos != nil {
		db.reclaimSize += int64(pos.Size)
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	//判断key的有效性
	if len(key) == 0 {
		return nil, selferror.ErrKeyIsEmpty
	}
	//从内存数据中取出key对应的索引信息
	logRecordPos := db.index.Get(key)

	//如果key不存在内存索引中，说明key不存在
	if logRecordPos == nil {
		return nil, selferror.ErrKeyNotFound
	}

	return db.getVauleByPosition(logRecordPos)
}

func (db *DB) getVauleByPosition(pos *data.LogRecordPos) ([]byte, error) {
	//根据文件id找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[pos.Fid]
	}

	//数据文件为空
	if dataFile == nil {
		return nil, selferror.ErrDataFileNotFound
	}

	//找到了对应的数据文件，并根据偏移量读取数据
	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, selferror.ErrKeyNotFound
	}
	return logRecord.Value, nil
}

// 追加写数据到活跃文件中,带锁
func (db *DB) appendLogRecordWithLock(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.appendLogRecord(record)
}

// 追加写数据到活跃文件中
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	//判断当前活跃数据文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	//如果为空，则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	//写入数据编码
	enRecord, size := data.EncodeLogRecord(record)
	//如果写入的数据已经到达了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.option.DataFileSize {
		//先持久化数据文件，保证已有的数据持久化到磁盘当中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//当前活跃文件转换为旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		//打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(enRecord); err != nil {
		return nil, err
	}

	db.bytesWrite += uint(size)
	//根据用户配置决定是否持久化
	var needSync = db.option.SyncWrites
	if !needSync && db.option.BytesPerSync > 0 && db.bytesWrite >= db.option.BytesPerSync { //如果该开关没有打开，再判断累计的字节是否超过
		needSync = true
	}
	if needSync {
		if err := db.activeFile.Sync(); err != nil {
			if db.bytesWrite > 0 {
				db.bytesWrite = 0
			}
			return nil, err
		}
	}

	//构造内存索引信息
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
		Size:   uint32(size),
	}
	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法前，必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	//打开新的数据文件
	dataFile, err := data.OpenDataFile(db.option.DirPath, initialFileId, fio.StandardFio)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFiles() error {
	dirEntries, err := os.ReadDir(db.option.DirPath)
	if err != nil {
		return err
	}
	var fileIds []int
	//遍历目录中的所有文件，找到所有以.data 结尾的文件

	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			//00001.data 进行分割，前面部分作为文件Id
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			//如果解析发生了错误，则数据目录存在其他文件，说明可能损坏掉了
			if err != nil {
				return selferror.ErrDataDirectoryCorrupte
			}
			fileIds = append(fileIds, fileId)
		}
	}

	//对文件ID进行排序，从小到大依次加载
	sort.Ints(fileIds)

	//赋值，为了后面加载索引的时候使用
	db.fileIds = fileIds

	//遍历每个文件Id，打开对应的数据文件
	for i, fid := range fileIds {
		ioType := fio.StandardFio
		if db.option.MMapAtStartup {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.option.DirPath, uint32(fid), ioType)
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { //最后一个，id是最大的，说明是当前活跃的文件
			db.activeFile = dataFile
		} else { //说明是旧的数据文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	//没有文件，说明数据库为空，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	//查看是否发生过Merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.option.DirPath, data.MergeFinishedFileName)
	_, err := os.Stat(mergeFinFileName)
	if err == nil {
		fid, err := db.getNonMergeFileId(db.option.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)
			db.reclaimSize += int64(pos.Size)
		} else {
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	//暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord) //这是一个以seqNo为key的list,value对应的是事务的记录
	var currentSeqNo uint64 = nonTransactionSeqNo

	//遍历所有文件的id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)

		//如果比最近未参与merge的文件id更小，则说明已经从hint文件中加载过了，直接跳过即可
		if hasMerge && fileId < nonMergeFileId {
			continue
		}

		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				//如果是读完的情况，跳出循环，其他错误则直接返回
				if err == io.EOF {
					break
				}
				return err
			}
			//构造内存索引，并保存
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}

			//解析 key，拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo { //如果是非事务提交的，则可以直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				//如果是事务完成提交的，则可以更新至内存索引
				if logRecord.Type == data.LogRecordTnxFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					//如果事务还没完成提交 or 没有提交成功，则先暂存起来
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			//更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			//递增offset，下一次从新的位置获取
			offset += size
		}

		//如果最后一个文件是当前活跃文件，更新这个文件的writeoff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	//更新事务序列号
	db.seqNo = currentSeqNo

	return nil
}

// 获取到所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	//遍历拿到所有Key的信息
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// 获取所有数据，并执行用户指定的操作,函数返回false,则终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getVauleByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

func (db *DB) Close() error {
	defer func() {
		err := db.fileLock.Unlock()
		if err != nil {
			panic(fmt.Sprintf("failed to unlock the directory,%v", err))
		}
	}()

	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()

	err := db.index.Close()
	if err != nil {
		return err
	}

	//保存当前事务的序列号
	seqNoFile, err := data.OpenSeqNoFile(db.option.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}
	encRecord, _ := data.EncodeLogRecord(record)
	err = seqNoFile.Write(encRecord)
	if err != nil {
		return err
	}

	err = seqNoFile.Sync()
	if err != nil {
		return err
	}

	//关闭当前活跃文件
	err = db.activeFile.Close()
	if err != nil {
		return err
	}

	//关闭旧的数据文件
	for _, file := range db.olderFiles {
		err := file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.option.DirPath, data.SeqNoFileName)
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.option.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.ReadLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true
	return nil
}

// 重置数据文件的IO类型为标准文件IO,将活跃文件，旧的数据文件都进行一次重置
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}
	err := db.activeFile.SetIoManager(db.option.DirPath, fio.StandardFio)
	if err != nil {
		return err
	}
	for _, dataFile := range db.olderFiles {
		err := dataFile.SetIoManager(db.option.DirPath, fio.StandardFio)
		if err != nil {
			return err
		}
	}
	return nil
}
