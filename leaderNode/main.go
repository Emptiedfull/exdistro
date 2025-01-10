package main

import (
	"net/http"
	_ "net/http/pprof"
	"strconv"
)

func main() {

	go func() {
		http.ListenAndServe("localhost:"+strconv.Itoa(Config.port+2), nil)
	}()

	node := NodeInit()
	go startClientApi(node)
	node.startListener()

}
