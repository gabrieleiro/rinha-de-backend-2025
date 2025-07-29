FROM golang:1.24

WORKDIR /usr/src/paymentq

COPY cmd/paymentq/main.go ./

EXPOSE 2222

RUN go build -o /usr/local/bin/paymentq main.go

CMD ["paymentq"]
