package main

import (
	bitcast_go "bitcast-go"
	"fmt"
)

func main() {
	options := bitcast_go.DefaultOptions
	options.DirPath = "/Users/yijun.dyj/GolandProjects/bitcast-go/temp"
	db, err := bitcast_go.Open(options)
	if err != nil {
		panic(err)
	}
	//err = db.Put([]byte("hello"), []byte("okokokokok"))
	//if err != nil {
	//	panic(err)
	//}
	val, err := db.Get([]byte("hello"))
	if err != nil {
		panic(err)
	}

	fmt.Println("val=", string(val))
}
