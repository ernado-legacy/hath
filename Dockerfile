FROM golang:1.4.2

COPY . /go/src/github.com/cydev/hath
WORKDIR /go/src/github.com/cydev/hath
RUN go get -d -v
RUN go install -v
CMD ["hath"] 
