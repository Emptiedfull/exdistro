package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"leader/pb"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
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
	perms       Permission
	nodeWriters map[net.Conn]chanPair
	bucket      *KvStore
	*keyPair

	nodeMux sync.RWMutex
	recMux  sync.RWMutex

	// activeReciepts map[uuid.UUID]map[net.Conn]*pb.Reciept
	// recMux         sync.Mutex

	recieptChan map[net.Conn]map[uuid.UUID]chan *pb.Reciept
}

type chanPair struct {
	msgCh chan []byte
	errCh chan bool
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
		nodeWriters: make(map[net.Conn]chanPair),
		// activeReciepts: make(map[uuid.UUID]map[net.Conn]*pb.Reciept),
		// recieptUpdate:  make([]chan bool, 0),
		recieptChan: map[net.Conn]map[uuid.UUID]chan *pb.Reciept{},
	}
	// go node.bucket.dumpProcess()

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
		go node.handleTls(conn)
	}
}

func createWriter(ch chan []byte, conn net.Conn, up chan bool) {
	for b := range ch {

		var length bytes.Buffer
		if err := binary.Write(&length, binary.BigEndian, uint32(len(b))); err != nil {
			fmt.Println("couldnt wite this shits length")
			continue
		}

		// fmt.Println(length)

		if _, err := length.Write(b); err != nil {
			fmt.Println("couldnt limit this shit")
		}

		_, err := conn.Write(length.Bytes())

		if err != nil {
			fmt.Println("error sending:", err)
			up <- false
		} else {
			up <- true
		}
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
	go node.establishTLS(conn)
}

func (node *Node) handleTls(conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 96)
	n, err := conn.Read(buf)
	if err != nil || n != 96 {

		fmt.Println("failed tls handshake recieve", err, n)
		return
	}

	verified := false
	for _, pub := range node.trusted {

		if ed25519.Verify(pub, buf[64:], buf[:64]) {
			verified = true
			break
		}
	}

	if !verified {
		fmt.Println("invalid tls signature")
		return
	}

	hash := ed25519.Sign(node.PrivateKey, buf[64:])

	conn.Write(append(hash, buf[64:]...))

	node.handleConnection(conn)
}

func (node *Node) establishTLS(conn net.Conn) {

	defer conn.Close()
	secret := make([]byte, 32)
	_, err := rand.Read(secret)
	if err != nil {
		fmt.Println("failed to generate scret")
		return
	}

	hash, err := node.keyPair.PrivateKey.Sign(nil, secret, &ed25519.Options{})
	if err != nil {
		fmt.Println("failed to sign")
		return
	}

	paylod := append(hash, secret...)

	_, err = conn.Write(paylod)
	if err != nil {
		fmt.Println("failed to write tls secret")
	}

	respBuf := make([]byte, 96)
	n, err := conn.Read(respBuf)
	if err != nil || n != 96 {
		fmt.Println("invalid tls handshake", err, n)
		return
	}

	verified := false
	for _, pub := range node.trusted {
		if ed25519.Verify(pub, respBuf[64:], respBuf[:64]) {
			verified = true

			break
		}
	}
	if !verified {
		fmt.Println("failed handshake")
		return
	}

	node.handleConnection(conn)

}

func (node *Node) handleConnection(conn net.Conn) {
	defer func() {
		fmt.Println("closing connection", conn.LocalAddr())
		node.nodeMux.Lock()
		delete(node.nodeWriters, conn)
		node.nodeMux.Unlock()
	}()

	fmt.Println("new node connected:", conn.LocalAddr())

	ch := make(chan []byte, 1024)
	er := make(chan bool)
	go createWriter(ch, conn, er)
	node.nodeWriters[conn] = chanPair{msgCh: ch, errCh: er}

	// buf := make([]byte, 1024)

	for {
		var length uint32

		err := binary.Read(conn, binary.BigEndian, &length)

		if err != nil {
			if nErr, ok := err.(net.Error); ok && nErr.Timeout() {
				continue
			}
			fmt.Println("error", err)
			return
		}
		message := make([]byte, length)
		if _, err := io.ReadFull(conn, message); err != nil {
			continue
		}

		go node.handleMessage(message, conn)
		// msg := make([]byte, n)
		// copy(msg, buf[:n])
		// go node.handleMessage(msg, conn)

	}

}

// func (node *Node) delimitMessages(stream []byte, conn net.Conn) {

// 	for len(stream) > 4 {
// 		length := binary.BigEndian.Uint32(stream[:4])
// 		fmt.Println(length, len(stream))
// 		if len(stream) < int(4+length) {
// 			break
// 		}
// 		message := stream[4 : 4+length]
// 		stream = stream[4+length:]

// 		go node.handleMessage(message, conn)
// 	}

// }

func (node *Node) handleMessage(msg []byte, conn net.Conn) {

	m, err := verifyMessage(msg)
	if err != nil {
		fmt.Println(err)
		return
	}
	if rec := m.GetReciept(); rec != nil {

		// if node.activeReciepts[uuid.UUID(m.GetUuid())] == nil {
		// 	node.activeReciepts[uuid.UUID(m.GetUuid())] = make(map[net.Conn]*pb.Reciept)
		// }
		// node.activeReciepts[uuid.UUID(m.GetUuid())][conn] = rec
		node.recMux.RLock()
		if ch := node.recieptChan[conn][uuid.UUID(m.GetUuid())]; ch != nil {
			node.recieptChan[conn][uuid.UUID(m.GetUuid())] <- rec
		}
		node.recMux.RUnlock()
		// for _, ch := range node.recieptUpdate {
		// 	ch <- true
		// }
		return
	}

	if ord := m.GetOrder(); ord != nil {
		rec := node.bucket.ProcessOrder(ord)

		Res := &pb.Msg{
			Uuid:    m.Uuid[:],
			Content: &pb.Msg_Reciept{Reciept: rec},
		}
		// reciept, err := proto.Marshal(&Res)
		// if err != nil {
		// 	fmt.Println(err)
		// 	return
		// }

		resp, err := signMessage(Res)
		if err != nil {
			fmt.Println(err)
			return
		}
		node.nodeWriters[conn].msgCh <- resp
		<-node.nodeWriters[conn].errCh

	}

}

func (node *Node) PropogateSetOrder(order *pb.Order) int32 {
	var wg sync.WaitGroup

	var errors atomic.Int32

	for conn := range node.nodeWriters {
		wg.Add(1)

		go func() {
			_, err := node.sendOrder(conn, order)
			if err != nil {
				errors.Add(1)
			}
			wg.Done()
		}()
	}
	return errors.Load()

}

func (node *Node) PropogateGetOrder(order *pb.Order) map[net.Conn]*pb.Reciept {
	var wg sync.WaitGroup

	reciepts := make(map[net.Conn]*pb.Reciept, len(node.nodeWriters))

	for conn := range node.nodeWriters {
		wg.Add(1)
		go func(conn net.Conn) {

			rec, _ := node.sendOrder(conn, order)
			reciepts[conn] = rec
			wg.Done()
		}(conn)

	}
	wg.Wait()
	return reciepts
}

func (node *Node) sendOrder(conn net.Conn, order *pb.Order) (*pb.Reciept, error) {
	txnID := uuid.New()

	m := &pb.Msg{
		Uuid:    txnID[:],
		Content: &pb.Msg_Order{Order: order},
	}

	msg, err := signMessage(m)

	if err != nil {
		return nil, err
	}

	if conn == nil {
		fmt.Println("not sending this order")
		return nil, fmt.Errorf("conn down")
	}

	// _, err = conn.Write(msg)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return nil, err
	// }

	node.nodeWriters[conn].msgCh <- msg
	res := <-node.nodeWriters[conn].errCh
	if !res {
		fmt.Println("failed sending")
		return nil, fmt.Errorf("fail")
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	up := make(chan *pb.Reciept, 1024)
	node.recMux.Lock()
	if node.recieptChan[conn] == nil {
		node.recieptChan[conn] = make(map[uuid.UUID]chan *pb.Reciept)
	}

	node.recieptChan[conn][txnID] = up
	node.recMux.Unlock()

	for {
		select {
		case <-timer.C:
			// node.nodes[conn.LocalAddr().String()] = nil
			fmt.Println("timing out,Node down", conn.LocalAddr())
			return nil, err
		case r := <-up:

			if r == nil {
				continue
			}
			return r, nil

			// if len(node.activeReciepts[txnID]) > 0 {
			// 	rec, exists := node.activeReciepts[txnID][conn]
			// 	if rec == nil || !exists {
			// 		continue
			// 	}

			// 	return rec, nil
			// }

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
