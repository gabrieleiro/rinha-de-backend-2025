FROM golang:1.24

WORKDIR /usr/src/loadbalancer

COPY cmd/loadbalancer/main.go ./

EXPOSE 9999

RUN go build -o /usr/local/bin/loadbalancer main.go

CMD ["loadbalancer"]
