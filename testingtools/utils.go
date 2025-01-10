package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

func main() {
	address := "http://localhost:8001/api/"

	ma := TimeXRequests(100000, address)
	for m, i := range ma {
		fmt.Println(m, i.Load())
	}

}

func TimeXRequests(x int, address string) map[int]*atomic.Int64 {
	start := time.Now()
	ma := XRequests(x, address)
	taken := time.Since(start)
	fmt.Println("total time taken:", taken)
	fmt.Println("Average response time:", taken/time.Duration(x))
	return ma
}

func XRequests(x int, address string) map[int]*atomic.Int64 {
	client := &fasthttp.Client{}

	statusMap := make(map[int]*atomic.Int64)

	for i := 0; i < x; i++ {
		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)
		req.SetRequestURI(address + strconv.Itoa(i) + "/" + strconv.Itoa(i*10290))
		req.Header.SetMethod("POST")

		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(resp)

		err := client.Do(req, resp)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}

		if i == 1 {
			fmt.Println(string(resp.Body()))
		}

		if i%(x/10) == 0 {
			fmt.Println(i, "requests done")
		}

		if _, exists := statusMap[resp.StatusCode()]; !exists {
			statusMap[resp.StatusCode()] = &atomic.Int64{}
		}
		statusMap[resp.StatusCode()].Add(1)

	}

	return statusMap
}

func testTls() {

	buf := make([]byte, 32)
	rand.Read(buf)

	pub, priv, _ := ed25519.GenerateKey(nil)

	h := ed25519.Sign(priv, buf)
	fmt.Println(len(h), len(buf))

	resBuf := make([]byte, 96)
	resBuf = append(h, buf...)

	hash := resBuf[:64]
	msg := resBuf[24:]

	fmt.Println(hash, msg)

	fmt.Println(ed25519.Verify(pub, msg, hash))

}
