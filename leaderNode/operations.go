package main

import (
	"fmt"
	"leader/pb"
	"sync"
	"time"
)

func (kv *KvStore) parseOrder(order *pb.Order) txnReciept {
	reciept := make(txnReciept, len(order.TxnList))

	var wg sync.WaitGroup

	timestamp := order.Timestamp
	for _, tx := range order.TxnList {
		wg.Add(1)
		go func(tx *pb.Txn) {
			defer wg.Done()

			r := kv.attemptTransaction(tx, timestamp)
			reciept = append(reciept, r)
		}(tx)
	}
	wg.Wait()
	fmt.Println(reciept)

	return reciept
}

func (kv *KvStore) attemptTransaction(txn *pb.Txn, timestamp int64) txnRecepitItem {

	rec := txnRecepitItem{timestamp: time.Now().Unix()}

	op := txn.Operation
	switch op {
	case pb.Operation(SET):
		s, err := kv.set(txn.Key, txn.Value, timestamp)
		rec.status = s
		rec.Operation = "SET"
		if err != nil {
			rec.Result = string(err.Error())
		}
	case pb.Operation_GET:
		s, v, err := kv.get(txn.Key)
		rec.status = s
		rec.Operation = "GET"
		if err != nil {
			rec.Result = string(err.Error())
		} else {
			rec.Result = string(v)
		}

	case pb.Operation_DELETE:
		fmt.Println("deleting")
	default:
		fmt.Println("defaulting")
	}

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
		return false, nil, fmt.Errorf("Not Found")
	} else {
		return true, val.val, nil
	}
}
