package main

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"leader/pb"
	"os"
	"sync"

	"github.com/google/uuid"
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

	fmt.Println(jsonMap, kv.db)

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

func signOrder(order *pb.Order, priv ed25519.PrivateKey) ([]byte, uuid.UUID, error) {

	bytz, err := proto.Marshal(order)
	if err != nil {
		return nil, uuid.Nil, err
	}
	hash := ed25519.Sign(priv, bytz)
	uid := uuid.New()
	r, _ := proto.Marshal(&pb.Msg{
		Hash:    hash,
		Uuid:    uid[:],
		Content: &pb.Msg_Order{Order: order},
	})

	return r, uid, nil
}

func verifyOrder(msg []byte, trusted []ed25519.PublicKey) (order *pb.Order, err error, txnId uuid.UUID) {
	m := pb.Msg{}
	err = proto.Unmarshal(msg, &m)
	if err != nil {
		return nil, err, uuid.Nil
	}

	hash := m.Hash
	byz, err := proto.Marshal(m.GetOrder())
	if err != nil {
		return nil, err, uuid.Nil
	}

	verified := false
	for _, pub := range trusted {
		if ed25519.Verify(pub, byz, hash) {
			verified = true
			break
		}
	}

	if !verified {
		return nil, fmt.Errorf("bad hash"), uuid.Nil
	}

	ord := m.GetOrder()

	return ord, nil, uuid.UUID(m.Uuid)

}
