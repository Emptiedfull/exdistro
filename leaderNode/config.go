package main

type config struct {
	keyStore string
	passkey  string
	port     int
	nodes    []string
}

var Config config = config{
	port:     9000,
	passkey:  "IVahscBV+w",
	keyStore: "localhost:12000",
	nodes: []string{
		"localhost:8000",
		"localhost:9000",
		"localhost:7000",
	},
}
