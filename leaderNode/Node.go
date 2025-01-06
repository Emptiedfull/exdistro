package main

import (
	"crypto/ed25519"
	"fmt"
	"leader/pb"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
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

	activeReciepts map[uuid.UUID]*pb.Reciept
	recieptUpdate  []chan bool
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

func NodeInit() *Node {
	// kp := generateKeyPair()
	kv := KvInit()

	node := Node{
		bucket: kv,
		// keyPair:   kp,
		nodes:          make(map[string]net.Conn),
		activeReciepts: make(map[uuid.UUID]*pb.Reciept),
		recieptUpdate:  make([]chan bool, 0),
	}

	err := node.getKeys()
	if err != nil {
		node.perms = READ
	} else {
		node.perms = WRITE
	}

	node.connect()

	fmt.Println("node permissions:", node.perms)

	return &node
}

func (node *Node) getKeys() error {
	conn, err := net.Dial("tcp", Config.keyStore)
	if err != nil {
		fmt.Println(err)
		return err
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
			fmt.Println("connect")
		}(Config.nodes[i])
	}

	wg.Wait()
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
		fmt.Println("closing connection", conn.LocalAddr())
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
			if nErr, ok := err.(net.Error); ok && nErr.Timeout() {

				continue
			}
			return
		}
		msg := make([]byte, n)
		copy(msg, buf[:n])
		go node.handleMessage(msg, conn)

	}

}

func (node *Node) handleMessage(msg []byte, conn net.Conn) {

	m := pb.Msg{}
	err := proto.Unmarshal(msg, &m)
	if err != nil {
		fmt.Println(err)
		return
	}
	if rec := m.GetReciept(); rec != nil {
		node.activeReciepts[uuid.UUID(m.GetUuid())] = rec
		for _, ch := range node.recieptUpdate {
			ch <- true
		}

		return
	}

	order, err, txnId := verifyOrder(msg, node.trusted)
	if err != nil {
		fmt.Println(err)
		return
	}

	rec := node.bucket.parseOrder(order)
	node.bucket.dump()

	Res := pb.Msg{
		Hash:    []byte("0"),
		Uuid:    txnId[:],
		Content: &pb.Msg_Reciept{Reciept: rec},
	}
	reciept, err := proto.Marshal(&Res)
	if err != nil {
		fmt.Println(err)
		return
	}
	conn.Write(reciept)

}

func (node *Node) PropogateOrder(conn net.Conn, order *pb.Order) {
	// conn.SetDeadline(time.Now().Add(5 * time.Second))

	fmt.Println("propogating order", order, node.PrivateKey)

	msg, txnID, err := signOrder(order, node.PrivateKey)

	if err != nil {
		return
	}

	_, err = conn.Write(msg)
	if err != nil {
		return
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	up := make(chan bool)
	node.recieptUpdate = append(node.recieptUpdate, up)

	for {
		select {
		case <-timer.C:
			// node.nodes[conn.LocalAddr().String()] = nil
			fmt.Println("failed to recieve reciept")
			return
		case <-up:
			fmt.Println("reciept update", node.activeReciepts[txnID].Items[0].Operation)
			if node.activeReciepts[txnID] == nil {
				continue
			}

			fmt.Println("acknowledgement recieved")
			return
			// default:
			// 	n, err := conn.Read(buf)
			// 	if err != nil {
			// 		fmt.Println(err)
			// 		continue
			// 	}

			// 	if uuid.UUID(m.GetUuid()) != txnID {
			// 		fmt.Println("bad uuid", m.GetUuid())
			// 		continue
			// 	}
			// 	fmt.Println("acknowledgement recieved")
			// 	return
		}

	}
}
