package main

import (
	"fmt"
	"leader/pb"
	"net"
	"sync"
	"time"
)

// func (kv *KvStore) parseOrder(order *pb.Order) *pb.Reciept {
// 	fmt.Println(order)
// 	Items := make([]*pb.RecieptItem, len(order.TxnList))

// 	var wg sync.WaitGroup

// 	timestamp := order.Timestamp
// 	for i, tx := range order.TxnList {
// 		wg.Add(1)
// 		go func(tx *pb.Txn) {
// 			defer wg.Done()

// 			r := kv.attemptTransaction(tx, timestamp)
// 			if r == nil {
// 				fmt.Println("couldnt attempt txn")
// 				return
// 			}
// 			Items[i] = r
// 		}(tx)
// 	}
// 	wg.Wait()

// 	receipt := &pb.Reciept{
// 		Items: Items,
// 	}

// 	return receipt
// }

// func (kv *KvStore) attemptTransaction(txn *pb.Txn, timestamp int64) *pb.RecieptItem {

// 	fmt.Println("operation", txn.Operation.Number())
// 	rec := &pb.RecieptItem{Timestamp: time.Now().Unix()}

// 	op := txn.Operation
// 	switch op {
// 	case pb.Operation(SET):
// 		s, err := kv.set(txn.Key, txn.Value, timestamp)
// 		rec.Operation = pb.Operation_SET
// 		fmt.Println("hha:", rec.Operation)
// 		rec.Status = s
// 		if err != nil {
// 			rec.Val = []byte(err.Error())
// 		} else {
// 			rec.Val = []byte("SUCCESS")
// 		}
// 	case pb.Operation_GET:
// 		s, v, err := kv.get(txn.Key)
// 		rec.Status = s
// 		if err != nil {
// 			rec.Val = []byte(err.Error())
// 		} else {
// 			rec.Val = v
// 		}

// 	case pb.Operation_DELETE:
// 		fmt.Println("deleting")
// 	default:
// 		fmt.Println("defaulting")
// 	}

// 	fmt.Println("rec:", &rec)
// 	fmt.Print("rec", rec.Operation)

// 	return rec
// }

func (kv *KvStore) ProcessOrder(order *pb.Order) (reciept *pb.Reciept) {
	Items := make([]*pb.RecieptItem, len(order.TxnList))

	switch order.Operation {
	case pb.Operation_GET:
		var wg sync.WaitGroup

		for i := 0; i < len(order.GetTxnList()); i++ {
			wg.Add(1)
			go func(txn *pb.Txn) {
				wg.Done()
				Items[i] = kv.ProccesGetTxn(txn)
			}(order.TxnList[i])
		}
		wg.Wait()
	case pb.Operation_SET:
		var wg sync.WaitGroup

		for i := 0; i < len(order.TxnList); i++ {
			wg.Add(1)
			go func(txn *pb.Txn) {
				wg.Done()
				Items[i] = kv.ProccesSetTxn(txn, order.Timestamp)
			}(order.TxnList[i])
		}
		wg.Wait()

	}
	rec := &pb.Reciept{
		Operation: order.Operation,
		Items:     Items,
	}

	return rec

}

func (kv *KvStore) ProccesGetTxn(txn *pb.Txn) (rec *pb.RecieptItem) {
	r := &pb.RecieptItem{}
	key := txn.GetKey()
	if key == "" {
		r.Status = false
		r.Key = "null"
		r.Error = "KeyErr"
		return r
	}
	r.Key = key

	val, err := kv.getInternal(txn.GetKey())
	if err != nil {
		r.Status = false
		r.Error = err.Error()
		return r
	}
	r.Status = true
	r.Val = val
	return r
}

func (kv *KvStore) ProccesSetTxn(txn *pb.Txn, timestamp int64) *pb.RecieptItem {
	r := &pb.RecieptItem{Key: txn.Key}
	err := kv.setInternal(txn.Key, txn.Value, timestamp)
	if err != nil {
		r.Status = false
		r.Error = err.Error()
	}
	r.Status = true
	return r
}

func (kv *KvStore) setInternal(key string, val []byte, timestamp int64) (err error) {
	kv.mux.Lock()
	defer kv.mux.Unlock()

	if _, exists := kv.db[key]; !exists {
		kv.db[key] = item{timestamp: timestamp, val: val}
		return nil
	} else {
		if kv.db[key].timestamp <= timestamp {
			kv.db[key] = item{timestamp: timestamp, val: val}
			return nil
		}
		return fmt.Errorf("OldVer")

	}
}

func (kv *KvStore) getInternal(key string) (val []byte, err error) {
	kv.mux.RLock()
	defer kv.mux.RUnlock()

	if val, exists := kv.db[key]; !exists {
		return nil, fmt.Errorf("Miss")
	} else {
		return val.val, nil
	}
}

func (node *Node) setClientOne(key string, val []byte) error {
	err := node.bucket.setInternal(key, val, time.Now().Unix())
	defer node.syncSetOne(key, val)
	if err != nil {
		return err
	}
	return nil
}

func (node *Node) syncSetOne(key string, val []byte) {
	order := &pb.Order{
		Timestamp: time.Now().Unix(),
		Operation: pb.Operation_SET,
		TxnList: []*pb.Txn{
			{
				Key:   key,
				Value: val,
			},
		},
	}

	node.PropogateSetOrder(order)
}

func (node *Node) getClientOne(key string) (val []byte, err error) {
	res, err := node.bucket.getInternal(key)
	if err == nil {
		return res, nil
	}

	order := &pb.Order{
		Operation: pb.Operation_GET,
		TxnList: []*pb.Txn{
			{
				Key: key,
			},
		},
	}

	reciepts := node.PropogateGetOrder(order)
	v, err := extractValOne(reciepts)
	if err != nil {
		return nil, err
	}
	defer func() {
		fmt.Println("self correcting")
		node.bucket.setInternal(key, v, time.Now().Unix())

	}()
	return v, nil

}

func (node *Node) setClientMass(m MassSet) error {
	var txnList []*pb.Txn

	for key, val := range m {
		txnList = append(txnList, &pb.Txn{Key: key, Value: val})
	}

	order := &pb.Order{
		Operation: pb.Operation_SET,
		Timestamp: time.Now().Unix(),
		TxnList:   txnList,
	}

	res := node.bucket.ProcessOrder(order)
	err := getSetStatus(res)
	if err != nil {
		return err
	}

	defer node.PropogateSetOrder(order)

	return nil
}

func getSetStatus(pb *pb.Reciept) error {
	suc := 0
	for _, item := range pb.Items {
		if item.GetStatus() {
			suc++
		}
	}

	if suc == 0 {
		return fmt.Errorf("No Items")
	}
	if suc < len(pb.Items) {
		return fmt.Errorf("Partial succes", suc)
	}
	return nil

}

// Consensus method
// func ensureValidity(reciepts map[net.Conn]*pb.Reciept) (val []byte, er error) {
// 	aggreing := make([]*net.Conn, 0, len(reciepts))
// 	non_ag := make([]*net.Conn, 0, len(reciepts))

// 	var pivot *pb.Reciept
// 	for _, p := range reciepts {
// 		pivot = p
// 		break
// 	}

// 	for conn, reciept := range reciepts {
// 		if reciept == pivot {
// 			aggreing = append(aggreing, &conn)
// 		} else {
// 			non_ag = append(non_ag, &conn)
// 		}
// 	}

// 	if len(aggreing) > len(non_ag) {
// 		return pivot.GetItems()[0].GetVal(), nil
// 	} else {
// 		return nil, fmt.Errorf("MISS")
// 	}
// }

func extractValOne(reciepts map[net.Conn]*pb.Reciept) (v []byte, er error) {
	if len(reciepts) <= 0 {
		return nil, fmt.Errorf("MISS:No reciept")
	}

	valueCount := make(map[string]int)

	majority := len(reciepts) / 2

	for _, reciept := range reciepts {
		fmt.Println("reciept", reciept)
		if len(reciept.GetItems()) <= 0 {
			continue
		}

		txn := reciept.GetItems()[0]

		if !txn.Status {
			continue
		}

		key := string(txn.Val)
		valueCount[key]++

		if valueCount[key] >= majority {
			return txn.Val, nil
		}
	}

	min := 1
	var vale string = ""
	for val, count := range valueCount {
		if count >= min {
			vale = val
		}
	}

	fmt.Println(valueCount)

	if vale == "" {
		return nil, fmt.Errorf("no concensus")
	}

	return []byte(vale), nil

}

func (node *Node) getClientMass(m MassGet) (res MassGetRes, err error) {

	txnList := make([]*pb.Txn, len(m))
	for i, key := range m {
		txnList[i] = &pb.Txn{
			Key: key,
		}
	}
	order := &pb.Order{
		Operation: pb.Operation_GET,
		Timestamp: time.Now().Unix(),
		TxnList:   txnList,
	}

	rec := node.bucket.ProcessOrder(order)
	msr := node.processMassGetReciept(rec)
	return msr, nil
}

func (node *Node) processMassGetReciept(reciept *pb.Reciept) (ms MassGetRes) {
	var unfinishedorders []*pb.RecieptItem
	var finishedorders []*pb.RecieptItem

	for _, item := range reciept.Items {
		if item.GetStatus() {
			finishedorders = append(finishedorders, item)
		} else {
			unfinishedorders = append(unfinishedorders, item)
		}
	}
	if len(unfinishedorders) == 0 {
		msr := make(MassGetRes)
		for _, item := range finishedorders {
			msr[item.Key] = item.Val
		}
		return msr
	}

	txnList := make([]*pb.Txn, len(unfinishedorders))

	for i, item := range unfinishedorders {
		txnList[i] = &pb.Txn{Key: item.Key}
	}

	order := &pb.Order{
		Timestamp: reciept.GetTimestamp(),
		Operation: pb.Operation_GET,
		TxnList:   txnList,
	}

	reciepts := node.PropogateGetOrder(order)

	if len(reciepts) == 0 {
		msr := make(MassGetRes)
		for _, item := range finishedorders {
			msr[item.Key] = item.Val
		}
		for _, item := range unfinishedorders {
			msr[item.Key] = []byte("missed")
		}
		return msr
	}

	missed := make(map[string]*pb.RecieptItem)
	hitted := make(map[string]*pb.RecieptItem)

	majority := len(node.nodes) / 2

	responses := make(map[string]map[string]int)

	for _, rec := range reciepts {
		for _, item := range rec.Items {

			if _, exists := hitted[item.Key]; exists {
				continue
			}
			if !item.Status {
				missed[item.Key] = item
				continue
			}
			missed[item.Key] = nil
			val := string(item.Val)
			if responses[item.Key] == nil {
				responses[item.Key] = make(map[string]int)
			}
			responses[item.Key][val]++

			if responses[item.Key][val] > majority {
				hitted[item.Key] = item
			}
		}
	}

	for key, response := range responses {
		max := 0
		var hit string
		for option, count := range response {
			if count > max {
				hit = option
				max = count
			}
		}
		hitted[key] = &pb.RecieptItem{Key: key, Val: []byte(hit)}
	}

	var MassGet MassGetRes = make(MassGetRes)

	for key, item := range hitted {
		MassGet[key] = item.Val
	}

	for key, item := range missed {
		MassGet[key] = []byte(item.GetError())
	}

	return MassGet

}
