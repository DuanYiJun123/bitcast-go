package main

import (
	bitcast_go "bitcast-go"
	"fmt"
)

func main() {
	options := bitcast_go.DefaultOptions
	options.DirPath = "/tmp/bitcast-go"
	db, err := bitcast_go.Open(options)
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("name"), []byte("bitcask"))
	if err != nil {
		panic(err)
	}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}

	fmt.Println("val=", string(val))
}
