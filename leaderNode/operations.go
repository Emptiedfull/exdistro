package main

import (
	"fmt"
	"leader/pb"
	"sync"
	"time"
)

func (kv *KvStore) parseOrder(order *pb.Order) *pb.Reciept {
	fmt.Println(order)
	Items := make([]*pb.RecieptItem, len(order.TxnList))

	var wg sync.WaitGroup

	timestamp := order.Timestamp
	for i, tx := range order.TxnList {
		wg.Add(1)
		go func(tx *pb.Txn) {
			defer wg.Done()

			r := kv.attemptTransaction(tx, timestamp)
			if r == nil {
				fmt.Println("couldnt attempt txn")
				return
			}
			Items[i] = r
		}(tx)
	}
	wg.Wait()

	receipt := &pb.Reciept{
		Items: Items,
	}

	return receipt
}

func (kv *KvStore) attemptTransaction(txn *pb.Txn, timestamp int64) *pb.RecieptItem {

	fmt.Println("operation", txn.Operation.Number())
	rec := &pb.RecieptItem{Timestamp: time.Now().Unix()}

	op := txn.Operation
	switch op {
	case pb.Operation(SET):
		s, err := kv.set(txn.Key, txn.Value, timestamp)
		rec.Operation = pb.Operation_SET
		fmt.Println("hha:", rec.Operation)
		rec.Status = s
		if err != nil {
			rec.Val = []byte(err.Error())
		} else {
			rec.Val = []byte("SUCCESS")
		}
	case pb.Operation_GET:
		s, v, err := kv.get(txn.Key)
		rec.Status = s
		if err != nil {
			rec.Val = []byte(err.Error())
		} else {
			rec.Val = v
		}

	case pb.Operation_DELETE:
		fmt.Println("deleting")
	default:
		fmt.Println("defaulting")
	}

	fmt.Println("rec:", &rec)
	fmt.Print("rec", rec.Operation)

	return rec
}

func (kv *KvStore) set(key string, val []byte, timestamp int64) (success bool, err error) {
	kv.mux.Lock()
	defer kv.mux.Unlock()

	if _, exists := kv.db[key]; !exists {
		kv.db[key] = item{timestamp: timestamp, val: val}
		return true, nil
	} else {
		if kv.db[key].timestamp <= timestamp {
			kv.db[key] = item{timestamp: timestamp, val: val}
			return true, nil
		}
		return false, fmt.Errorf("newer version found")

	}
}

func (kv *KvStore) get(key string) (success bool, val []byte, err error) {
	kv.mux.RLock()
	defer kv.mux.RUnlock()

	if val, exists := kv.db[key]; !exists {
		return false, nil, fmt.Errorf("not Found")
	} else {
		return true, val.val, nil
	}
}
