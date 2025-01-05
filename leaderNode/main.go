package main

import (
	"fmt"
	"leader/pb"
	"time"

	"google.golang.org/protobuf/proto"
)

func main() {

	node := NodeInit()
	order := &pb.Order{
		Timestamp: time.Now().Unix(),
		TxnList: []*pb.Txn{
			{
				Operation: 0,
				Key:       "he2y",
				Value:     []byte("jojo"),
			},
		},
	}

	m, err := proto.Marshal(order)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(m, node)

	node.startListener()

}
