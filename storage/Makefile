all: format check test
check:
	gometalinter --enable lll --enable goimports --enable "gofmt -s"
format:
	goimports -w .
test:
	go test
bench:
	go test -benchmem -bench .
escape:
	go build -gcflags '-m -l'
