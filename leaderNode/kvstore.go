package main

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"leader/pb"
	"os"
	"sync"

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

func verifyMessage(res []byte, trusted []ed25519.PublicKey) (m *pb.Msg, er error) {
	msg := &pb.Msg{}
	err := proto.Unmarshal(res, msg)
	if err != nil {
		return nil, err
	}

	var bytez []byte
	switch msg.GetContent().(type) {
	case *pb.Msg_Order:
		bytez, err = proto.Marshal(msg.GetOrder())
		if err != nil {
			return nil, err
		}
	case *pb.Msg_Reciept:
		bytez, err = proto.Marshal(msg.GetReciept())
		if err != nil {
			return nil, err
		}
	}

	verified := false
	for _, key := range trusted {
		if ed25519.Verify(key, bytez, msg.Hash) {
			verified = true
			break
		}
	}
	if !verified {
		return nil, fmt.Errorf("BAD HASH")
	}
	return msg, nil
}

func signMessage(msg *pb.Msg, priv ed25519.PrivateKey) (load []byte, er error) {
	var bytez []byte
	var err error
	switch msg.GetContent().(type) {
	case *pb.Msg_Order:
		bytez, err = proto.Marshal(msg.GetOrder())
		if err != nil {
			return nil, err
		}
	case *pb.Msg_Reciept:
		bytez, err = proto.Marshal(msg.GetReciept())
		if err != nil {
			return nil, err
		}
	}

	hash := ed25519.Sign(priv, bytez)
	msg.Hash = hash

	payload, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return payload, nil

}
