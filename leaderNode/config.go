package main

type config struct {
	keyStore string
	passkey  string
	port     int
	nodes    []string
}

var Config config = config{
	port:     8000,
	passkey:  "oLBTA7gHDh",
	keyStore: "localhost:12000",
	nodes: []string{
		"localhost:9000",
	},
}
