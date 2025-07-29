FROM golang:1.24

WORKDIR /usr/src/api

COPY cmd/api/main.go ./
COPY go.mod ./
COPY go.sum ./

EXPOSE 8888
EXPOSE 7777

RUN go build -o /usr/local/bin/api main.go

CMD ["api"]
