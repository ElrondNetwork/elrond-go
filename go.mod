module github.com/ElrondNetwork/elrond-go

go 1.13

require (
	github.com/ElrondNetwork/arwen-wasm-vm v1.2.19
	github.com/ElrondNetwork/arwen-wasm-vm/v1_3 v1.3.19
	github.com/ElrondNetwork/concurrent-map v0.1.3
	github.com/ElrondNetwork/elastic-indexer-go v1.0.6
	github.com/ElrondNetwork/elrond-go-logger v1.0.4
	github.com/beevik/ntp v0.3.0
	github.com/btcsuite/btcd v0.22.0-beta
	github.com/btcsuite/btcutil v1.0.3-0.20201208143702-a53e38424cce
	github.com/davecgh/go-spew v1.1.1
	github.com/denisbrodbeck/machineid v1.0.1
	github.com/elastic/go-elasticsearch/v7 v7.12.0
	github.com/gin-contrib/cors v0.0.0-20190301062745-f9e10995c85a
	github.com/gin-contrib/pprof v1.3.0
	github.com/gin-gonic/gin v1.7.2
	github.com/gizak/termui/v3 v3.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/gops v0.3.18
	github.com/gorilla/websocket v1.4.2
	github.com/hashicorp/golang-lru v0.5.4
	github.com/herumi/bls-go-binary v0.0.0-20200324054641-17de9ae04665
	github.com/ipfs/go-log v1.0.5
	github.com/jbenet/goprocess v0.1.4
	github.com/libp2p/go-libp2p v0.13.0
	github.com/libp2p/go-libp2p-core v0.8.5
	github.com/libp2p/go-libp2p-kad-dht v0.11.1
	github.com/libp2p/go-libp2p-kbucket v0.4.7
	github.com/libp2p/go-libp2p-pubsub v0.4.1
	github.com/libp2p/go-libp2p-secio v0.2.2
	github.com/mitchellh/mapstructure v1.4.1
	github.com/mr-tron/base58 v1.2.0
	github.com/multiformats/go-multiaddr v0.3.1
	github.com/pelletier/go-toml v1.9.0
	github.com/pkg/errors v0.9.1
	github.com/shirou/gopsutil v0.0.0-20190901111213-e4ec7b275ada
	github.com/stretchr/testify v1.7.0
	github.com/syndtr/goleveldb v1.0.1-0.20190318030020-c3a204f8e965
	github.com/urfave/cli v1.22.5
	github.com/whyrusleeping/timecache v0.0.0-20160911033111-cfcb2f1abfee
	golang.org/x/crypto v0.0.0-20200622213623-75b288015ac9
	golang.org/x/net v0.0.0-20200519113804-d87ec0cfa476
	gopkg.in/go-playground/validator.v8 v8.18.2
)

replace github.com/gogo/protobuf => github.com/ElrondNetwork/protobuf v1.3.2

replace github.com/ElrondNetwork/arwen-wasm-vm/v1_3 v1.3.19 => github.com/ElrondNetwork/arwen-wasm-vm v1.3.19

replace github.com/ElrondNetwork/elastic-indexer-go => github.com/ElrondNetwork/elastic-indexer-go v1.0.6-0.20210628101121-ddae861ac394
