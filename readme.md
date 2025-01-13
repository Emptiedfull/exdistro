# ExStore

ExStore is a multi nodal distributed in memory key value database built in on. It is built on the fundamentals of performance and reliablity.


## Features

- Secure Communication through assymetric cryptography
- Extensive optional logging facilities
- Lightweight communication with protobuf
- Acknowledgement handling
- Zero Allocation request handling
- Easy setup 
- Centralized key authority

## Getting Started

### Prerequisites

- Go 1.19+
- Fiber

## Usage

1. Start the keystore
    ```bash
    ./keystore
    ```

2. Grab the key from the output, for example `u2EVXetcQm`

3. Input the key into config.go

    ```go 
    var Config config = config{
        port:     9000,
        passkey:  "<passkey>",
        keyStore: "localhost:12000",
        nodes: []string{
            "localhost:8000",
            "localhost:9000",
            "localhost:7000",
        },
    }
    ```
4. Start the node 
    ```bash 
    go run *.go
    ```

### Endpoints

- GET /ping - Health check
- POST /api/:key/:value - Set value
- POST /api/massSet - Set more than one value(takes json input)
- GET /api/:key - Get value
- DELETE /api/:key - Delete key


