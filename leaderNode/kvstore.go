package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
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

type txnRecepitItem struct {
	timestamp int64
	Operation string
	status    bool
	Result    string
}

type txnReciept []txnRecepitItem

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

	os.WriteFile("kvstore_dump.json", data, 0644)
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
