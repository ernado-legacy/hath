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
