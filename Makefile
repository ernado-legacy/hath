all: install build

build:
	go build .
install:
	go get -v -d .

docker-latest:
	docker run -v "$(PWD)":/go/hath -w /go/hath golang:1.5 make

docker:
	docker run -v "$(PWD)":/go/hath -w /go/hath golang:1.4.2 make

image:
	docker build -t hath .

docker-cross:
	docker build -t go-cross crosscompile
	docker run -v "$(PWD)":/go/hath -w /go/hath go-cross make crosscompile

cross:
	goxc -d build -bc="windows, linux, darwin"
