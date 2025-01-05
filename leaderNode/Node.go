package main

import (
	"crypto/ed25519"
	"fmt"
	"leader/pb"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

type Permission uint8

const (
	READ Permission = 1 << iota
	WRITE
)

type Node struct {
	perms    Permission
	nodes    map[string]net.Conn
	listener net.Conn
	bucket   *KvStore
	*keyPair

	nodeMux sync.RWMutex

	msgBuffer chan []byte
}

type keyPair struct {
	trusted    []ed25519.PublicKey
	PrivateKey ed25519.PrivateKey
}

// func generateKeyPair() *keyPair {
// 	pub, priv, _ := ed25519.GenerateKey(nil)

// 	return &keyPair{
// 		PublicKey:  pub,
// 		PrivateKey: priv,
// 	}
// }

func signOrder(order *pb.Order, priv ed25519.PrivateKey) ([]byte, error) {

	bytz, err := proto.Marshal(order)
	if err != nil {
		return nil, err
	}
	hash := ed25519.Sign(priv, bytz)
	r, _ := proto.Marshal(&pb.Msg{
		Hash:  hash,
		Order: bytz,
	})
	return r, nil
}

func verifyOrder(msg []byte, pub ed25519.PublicKey) (*pb.Order, error) {
	m := pb.Msg{}
	err := proto.Unmarshal(msg, &m)
	if err != nil {
		return nil, fmt.Errorf("bad msg", err)
	}
	valid := ed25519.Verify(pub, m.Order, m.Hash)
	if !valid {
		return nil, fmt.Errorf("bad signature")
	}
	o := pb.Order{}
	err = proto.Unmarshal(m.Order, &o)
	if err != nil {
		return nil, fmt.Errorf("Bad order")
	}
	return &o, nil

}

func NodeInit() *Node {
	// kp := generateKeyPair()
	kv := KvInit()

	node := Node{
		bucket: kv,
		// keyPair:   kp,
		nodes:     make(map[string]net.Conn),
		msgBuffer: make(chan []byte, 10),
	}

	err := node.getKeys()
	if err != nil {
		node.perms = READ
	} else {
		node.perms = WRITE
	}

	go node.connect()

	return &node
}

func (node *Node) getKeys() error {
	conn, err := net.Dial("tcp", Config.keyStore)
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()

	conn.Write([]byte(Config.passkey))

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err)
		return err
	}

	payload := pb.Payload{}
	if err := proto.Unmarshal(buf[:n], &payload); err != nil {
		fmt.Println(err)
		return err
	}

	kP, err := parserPayload(&payload)
	if err != nil {
		fmt.Println()
		return err
	}

	node.keyPair = &kP
	return nil

}

func parserPayload(payload *pb.Payload) (keyPair, error) {
	kP := keyPair{}
	if len(payload.PrivKey) != ed25519.PrivateKeySize {
		return kP, fmt.Errorf("bad payload")
	}
	kP.PrivateKey = payload.PrivKey

	for _, pub := range payload.Trusted {
		if len(pub) == ed25519.PublicKeySize {
			kP.trusted = append(kP.trusted, pub)
		}
	}
	return kP, nil
}

func (node *Node) startListener() {
	listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(Config.port))
	if err != nil {
		panic(fmt.Sprintf("Failed to start listener: %v", err))
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go node.handleConnection(conn)
	}
}

func (node *Node) connect() {
	var wg sync.WaitGroup

	for i := 0; i < len(Config.nodes); i++ {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()
			node.connectTo(address)
		}(Config.nodes[i])
	}

	wg.Wait()
	return
}

func (node *Node) connectTo(address string) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Println(err)
		return
	}
	go node.handleConnection(conn)
}

func (node *Node) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		node.nodeMux.Lock()
		node.nodes[conn.LocalAddr().Network()] = nil
		node.nodeMux.Unlock()
	}()

	fmt.Println("new node connected:", conn.LocalAddr())

	node.nodes[conn.LocalAddr().String()] = conn

	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		msg := make([]byte, n)
		copy(msg, buf[:n])
		go node.handleMessage(msg, conn)

	}

}

func (node *Node) handleMessage(msg []byte, conn net.Conn) {

}
