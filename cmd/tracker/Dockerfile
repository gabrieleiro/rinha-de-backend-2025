FROM golang:1.24

WORKDIR /usr/src/tracker

COPY main.go ./

EXPOSE 1111/udp

RUN go build -o /usr/local/bin/tracker main.go

CMD ["tracker"]
