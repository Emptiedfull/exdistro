package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"keystore/pb"
	"net"

	"google.golang.org/protobuf/proto"
)

type KeyStore struct {
	passkey     string
	trustedKeys [][]byte
	nodeKeys    [][]byte
}

func (kS *KeyStore) init(n int) {
	for i := 0; i <= n; i++ {
		pub, priv, _ := ed25519.GenerateKey(nil)
		kS.trustedKeys = append(kS.trustedKeys, pub)
		kS.nodeKeys = append(kS.nodeKeys, priv)
	}

}

func (kS *KeyStore) generateKeyPair() ed25519.PrivateKey {
	if len(kS.nodeKeys) > 0 {
		priv := kS.nodeKeys[0]
		kS.nodeKeys = kS.nodeKeys[1:]
		return priv
	}
	return nil
}

func createPassKey() string {
	bytes := make([]byte, 10)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return base64.StdEncoding.EncodeToString(bytes)[:10]
}

func main() {

	key := createPassKey()
	fmt.Println("the key is:", key)

	KeyS := KeyStore{passkey: key}
	KeyS.init(10)
	KeyS.startserver()

}

func (kS *KeyStore) startserver() {
	listener, err := net.Listen("tcp", "localhost:12000")
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go kS.handleConnection(conn)
	}

}

func (kS *KeyStore) handleConnection(conn net.Conn) {
	buf := make([]byte, 128)

	n, err := conn.Read(buf)
	if err != nil {
		return
	}

	pass := bytes.TrimSpace(buf[:n])
	if string(pass) == kS.passkey {
		priv := kS.generateKeyPair()
		if priv == nil {
			return
		}
		load := &pb.Payload{
			PrivKey: priv,
			Trusted: kS.trustedKeys,
		}
		py, err := proto.Marshal(load)
		if err != nil {
			fmt.Println(err)
			return
		}

		conn.Write(py)
	} else {
		fmt.Println("denied")
	}
}
