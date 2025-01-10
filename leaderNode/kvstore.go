package main

import (
	"encoding/json"
	"fmt"
	"leader/pb"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

type Operation int

const (
	SET Operation = iota
	GET
	DELETE
)

type KvStore struct {
	db  map[string]item
	mux sync.RWMutex
}

type item struct {
	val       []byte
	timestamp int64
}

func KvInit() *KvStore {

	existing, _ := os.ReadFile("kvstore_dump.json")
	if existing != nil {
		fmt.Println("digesting")
		return digest(existing)
	} else {
		return &KvStore{
			db: make(map[string]item),
		}
	}

}

func (kv *KvStore) dump() {
	kv.mux.RLock()
	defer kv.mux.RUnlock()

	type jsonItem struct {
		Value     string `json:"value"`
		Timestamp int64  `json:"timestamp"`
	}

	jsonMap := make(map[string]jsonItem)
	for k, v := range kv.db {
		jsonMap[k] = jsonItem{
			Value:     string(v.val),
			Timestamp: v.timestamp,
		}
	}

	data, err := json.MarshalIndent(jsonMap, "", "    ")
	if err != nil {
		return
	}

	os.WriteFile(strconv.Itoa(Config.port)+"kvstore_dump.json", data, 0644)
}

func digest(data []byte) *KvStore {
	kv := KvStore{db: make(map[string]item)}
	type jsonItem struct {
		Value     string `json:"value"`
		Timestamp int64  `json:"timestamp"`
	}

	jsonMap := make(map[string]jsonItem)

	if err := json.Unmarshal(data, &jsonMap); err != nil {
		return &kv
	}

	for k, v := range jsonMap {
		kv.db[k] = item{
			val:       []byte(v.Value),
			timestamp: v.Timestamp,
		}
	}

	return &kv

}

func verifyMessage(res []byte) (m *pb.Msg, er error) {
	msg := &pb.Msg{}
	err := proto.Unmarshal(res, msg)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func signMessage(msg *pb.Msg) (load []byte, er error) {

	payload, err := proto.Marshal(msg)
	if err != nil {
		fmt.Println("error marshaling", err)
		return nil, err
	}
	return payload, nil

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

func (kv *KvStore) delInternal(key string) error {
	kv.mux.Lock()
	defer kv.mux.Unlock()

	if _, exists := kv.db[key]; exists {
		delete(kv.db, key)
	} else {
		return fmt.Errorf("null")
	}
	return nil
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

func (kv *KvStore) dumpProcess() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		kv.dump()
	}
}
