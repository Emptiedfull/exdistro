package main

import (
	"fmt"
	"leader/pb"
	"time"
)

func main() {

	node := NodeInit()
	defer node.bucket.dump()
	order := &pb.Order{
		Timestamp: time.Now().Unix(),
		TxnList: []*pb.Txn{
			{
				Operation: pb.Operation_SET,
				Key:       "he2y",
				Value:     []byte("jojo"),
			},
		},
	}
	time.Sleep(1 * time.Second)
	fmt.Println(node.nodes)
	for _, conn := range node.nodes {
		node.PropogateOrder(conn, order)
	}

	node.startListener()

}
