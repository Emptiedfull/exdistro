package main

type config struct {
	keyStore string
	passkey  string
	port     int
	nodes    []string
}

var Config config = config{
	port:     9000,
	passkey:  "+JqZkcJsZF",
	keyStore: "localhost:12000",
	nodes: []string{
		"localhost:8000",
	},
}
