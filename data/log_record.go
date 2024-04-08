package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

//crc type keySize valueSize

//4 + 1 + 5 + 5 = 15
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

//LogRecordPos 数据存储索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件id，表示存储在了哪个文件中
	Offset int64  //表示数据在文件中的具体位置
}

// LogRecord 写入到数据文件的记录
//之所以叫日志，是因为数据文件中的数据是追加写入的。类似日志的格式
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType //枚举，用于记录数据的状态
}

// EncodeLogRecord 对LogRecord进行编码，返回字节数组以及长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
