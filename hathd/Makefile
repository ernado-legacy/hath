all: install build

build:
	go build .
install:
	go get -v -d .

docker-latest:
	docker run -v "$(PWD)":/go/hathd -w /go/hathd golang:1.5 make

docker:
	docker run -v "$(PWD)":/go/hathd -w /go/hathd golang:1.4.2 make

image:
	docker build -t cydev/hath .

docker-cross:
	docker build -t go-cross crosscompile
	docker run -v "$(PWD)":/go/hathd -w /go/hathd go-cross make crosscompile

cross:
	goxc -d build -bc="windows, linux, darwin"
