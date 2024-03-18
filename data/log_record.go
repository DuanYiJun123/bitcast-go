package data

//LogRecordPos 数据存储索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件id，表示存储在了哪个文件中
	Offset int64  //表示数据在文件中的具体位置
}
