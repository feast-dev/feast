module github.com/feast-dev/feast

go 1.17

require (
	github.com/apache/arrow/go/v7 v7.0.0
	github.com/ghodss/yaml v1.0.0
	github.com/go-python/gopy v0.4.0
	github.com/go-redis/redis/v8 v8.11.4
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.2.0
	github.com/mattn/go-sqlite3 v1.14.12
	github.com/spaolacci/murmur3 v1.1.0
	github.com/stretchr/testify v1.7.0
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20220315005136-aec0fe3e777c
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
)

require (
	github.com/apache/arrow/go/arrow v0.0.0-20200730104253-651201b0f516 // indirect
	github.com/apache/thrift v0.16.0 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/goccy/go-json v0.7.10 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v2.0.0+incompatible // indirect
	github.com/google/go-cmp v0.5.7 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.9 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/mod v0.5.1-0.20210830214625-1b1db11ec8f4 // indirect
	golang.org/x/net v0.0.0-20211015210444-4f30a5c0130f // indirect
	golang.org/x/sys v0.0.0-20211025201205-69cdffdb9359 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.4 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/genproto v0.0.0-20220118154757-00ab72f36ad5 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)

replace github.com/go-python/gopy v0.4.0 => github.com/pyalex/gopy v0.4.1-0.20220315220227-146bcd99e9f5
