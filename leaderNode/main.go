package main

import (
	"fmt"
	"leader/pb"
	"time"
)

func main() {

	node := NodeInit()
	order := &pb.Order{
		Timestamp: time.Now().Unix(),
		Operation: pb.Operation_SET,
		TxnList: []*pb.Txn{
			{

				Key:   "he2y",
				Value: []byte("jojo"),
			},
		},
	}
	time.Sleep(1 * time.Second)
	fmt.Println(node.nodes)
	down := node.PropogateSetOrder(order)
	fmt.Println(down)
	go startClientApi(node)
	node.startListener()

}
