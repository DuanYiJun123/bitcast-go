package data

import (
	"encoding/binary"
	"hash/crc32"
)

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

// EncodeLogRecord 对LogRecord进行编码，返回字节数组以及长度（需要对header信息编码为字节数组，因为key和value本身就是字节数组，无需编解码）
// crc校验值 / type类型 / key size / value size / key / value
//    4字节      1字节      变长（最大5）           变长
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	//初始化一个头部信息的header字节数组
	header := make([]byte, maxLogRecordHeaderSize)

	//从第五个字节存储type
	header[4] = logRecord.Type
	var index = 5
	//5字节之后，存储的是key和value的长度信息
	//使用变长类型，节省空间
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	var size = index + len(logRecord.Key) + len(logRecord.Value) //编码之后的长度就是header的长度+key长度+value长度
	encBytes := make([]byte, size)

	//将header部分内容拷贝过来
	copy(encBytes[:index], header[:index])
	//将Key和value数据拷贝到字节数组中
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	//对整个LogRecord的数据进行crc校验
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
