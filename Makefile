PACKAGES=`go list ./... | grep -v example`

test:
	go test -v -cover ${PACKAGES}

format:
	go fmt github.com/sabey/parquet-go/...

.PHONEY: test
