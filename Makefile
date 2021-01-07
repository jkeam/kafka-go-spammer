.PHONY: build
build:
	go build -o bin/main main.go

.PHONY: run
run:
	go run main.go

.PHONY: compile
compile:
	echo "Compiling for every OS and Platform"
	GOOS=linux GOARCH=arm go build -o bin/main-linux-arm main.go
	GOOS=linux GOARCH=arm64 go build -o bin/main-linux-arm64 main.go
	GOOS=freebsd GOARCH=386 go build -o bin/main-freebsd-386 main.go

.PHONY: clean
clean:
	go clean
