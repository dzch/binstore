# binstore

binstore是一个二进制存储系统的组件，用以存取二进制数据。

# Build

    go get github.com/dzch/binstore
	cd ${GOPATH}/src/github.com/dzch/binstore
	go build binstore_bin.go

# Config

请参考docker/binstore.yaml

# Run

	./binstore_bin -f binstore.yaml
