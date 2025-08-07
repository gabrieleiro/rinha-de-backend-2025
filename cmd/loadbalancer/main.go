package main

import (
	"bytes"
	"errors"
	"fmt"
	"os/signal"
	"sync/atomic"
	"syscall"
	"unicode"

	"log"
	"net"
	"os"
	"strings"

	"github.com/lesismal/nbio"
)

var POST = []byte("POST")
var GET = []byte("GET")

var HTTP_200_OK = []byte("HTTP/1.1 200 OK\r\n")
var HTTP_200_OK_RETURN = []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
var HTTP_500_Internal_Server_Error = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n")
var HTTP_404_NOT_FOUND = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")

var BODY_SEPARATOR = []byte("\r\n\r\n")

var PAYMENTS_ENDPOINT = []byte("/payments")
var PAYMENTS_SUMMARY_ENDPOINT = []byte("/payments-summary")

var SERVERS = []ServerInfo{}
var pickerIndex uint32

type ServerInfo struct {
	Address           string
	Conn              *net.UnixConn
	UnixSocketAddress *net.UnixAddr
}

func pickServer() *ServerInfo {
	// I mean... it's just two servers anyway
	return &SERVERS[int(pickerIndex)%len(SERVERS)]
}

// zero-allocation json parser
// only parses "correlationId" and "amount" fields
func parseJson(buffer []byte) ([]byte, []byte, error) {
	var correlationId, amount []byte
	var c byte
	var pos int

	advance := func() {
		pos++
		c = buffer[pos]
	}

	for pos = 0; pos < len(buffer); pos++ {
		c = buffer[pos]

		for c == ' ' || c == '\n' || c == '\t' {
			advance()
		}

		if c != '"' {
			continue
		}

		advance()

		if c == 'c' { // correlationId
			for c != ':' {
				advance()
			}

			for c != '"' {
				advance()
			}

			advance() // skip opening "

			stringStart := pos

			for c != '"' {
				advance()
			}

			correlationId = buffer[stringStart:pos]
		} else if c == 'a' { // amount
			for c != ':' {
				advance()
			}

			for !unicode.IsDigit(rune(c)) {
				advance()
			}

			numberStart := pos

			for unicode.IsDigit(rune(c)) {
				advance()
			}

			if c == '.' {
				advance()
			}

			for unicode.IsDigit(rune(c)) {
				advance()
			}

			var err error
			amount = buffer[numberStart:pos]

			if err != nil {
				return correlationId, amount, errors.New("parsing float")
			}
		}
	}

	return correlationId, amount, nil
}

// expects URI encoded query args
// like ?name=tom&enemy=jerry
func parseQueryArgs(input []byte) map[string]string {
	if len(input) == 0 {
		return nil
	}

	res := make(map[string]string)
	pos := 0
	c := input[pos]
	var field strings.Builder
	var value strings.Builder
	curr := &field

	for pos < len(input) && c != '\r' && c != ' ' {
		if c == '&' {
			res[field.String()] = value.String()
			field.Reset()
			value.Reset()

			curr = &field
		} else if c == '=' {
			curr = &value
		} else {
			curr.WriteByte(c)
		}

		pos++

		if pos < len(input) {
			c = input[pos]
		}
	}

	if value.Len() > 0 {
		res[field.String()] = value.String()
	}

	return res
}

// According to HTTP 1.1 RFC (2616), "Request Line"
// is the first line of an HTTP request,
// which is formatted as such:
// VERB /path/to/resource HTTP/1.1
//
// https://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html
func parseRequestLine(request []byte) (verb []byte, uri []byte, queryArgs map[string]string, err error) {
	if len(request) == 0 {
		return
	}

	pos := 0
	c := request[pos]

	advance := func() {
		pos++

		if pos < len(request) {
			c = request[pos]
		}
	}

	for c != ' ' {
		verb = append(verb, c)
		advance()
	}

	advance()

	for pos < len(request) && c != ' ' && c != '?' {
		uri = append(uri, c)
		advance()
	}

	if pos < len(request) && c == '?' {
		advance()
		queryArgs = parseQueryArgs(request[pos:])
	}

	return
}

func bodyFrom(request []byte) []byte {
	for i := range request {
		if request[i] == '\r' && bytes.Equal(request[i:i+4], BODY_SEPARATOR) {
			return request[i+4:]
		}
	}

	return nil
}

func main() {
	serverAddresses := strings.Split(os.Getenv("SERVERS"), ",")

	if len(serverAddresses) < 1 {
		log.Printf("no server addresses specified\n")
		return
	}

	tcpAddress := os.Getenv("TCP_ADDRESS")
	if tcpAddress == "" {
		tcpAddress = ":9999"
	}

	unixSocketPath := os.Getenv("UNIX_SOCKET_ADDRESS")
	if unixSocketPath == "" {
		unixSocketPath = "./sockets/rinha_load_balancer.sock"
	}

	os.Remove(unixSocketPath)

	var err error
	unixAddr, err := net.ResolveUnixAddr("unixgram", unixSocketPath)
	if err != nil {
		log.Printf("resolving address: %v\n", err)
		return
	}

	unixgramConn, err := net.ListenUnixgram("unixgram", unixAddr)
	if err != nil {
		log.Printf("listening unix socket: %v\n", err)
		return
	}

	// set up connection to servers
	for _, sa := range serverAddresses {
		si := ServerInfo{Address: sa}
		si.UnixSocketAddress, err = net.ResolveUnixAddr("unixgram", sa)
		if err != nil {
			log.Printf("resolving server URI: %v\n", err)
			continue
		}
		SERVERS = append(SERVERS, si)
	}

	engine := nbio.NewEngine(nbio.Config{
		Network:        "tcp",
		Addrs:          []string{tcpAddress},
		ReadBufferSize: 512,
	})

	engine.OnData(func(c *nbio.Conn, data []byte) {
		s := pickServer()
		atomic.AddUint32(&pickerIndex, 1)

		verb, uri, queryArgs, err := parseRequestLine(data)
		if err != nil {
			log.Printf("parsing request line: %v\n", err)
			return
		}

		if bytes.Equal(verb, POST) && bytes.Equal(uri, PAYMENTS_ENDPOINT) {
			// POST doesn't return anything
			// and is processed assynchronously
			// so we just return early
			_, err = c.Write(HTTP_200_OK_RETURN)
			if err != nil {
				log.Printf("writing response: %v\n", err)
				return
			}

			correlationId, amount, err := parseJson(bodyFrom(data))
			if err != nil {
				log.Printf("parsing json: %v\n", err)
				return
			}

			if len(correlationId) == 0 || len(amount) == 0 {
				log.Printf("post body missing fields\n")
				return
			}

			// Formatting payload to send to backend server
			// The payload format is this:
			// <one byte>correlationId;amount\n
			// The first byte indicates whether we're
			// adding a new payment to be processed or
			// we want to retrieve a summary of payments
			// 0x0 = process new payment
			// 0x1 = get summary

			messageBuf := make([]byte, 0, len(correlationId)+len(amount)+3) // 3 = first byte + ; + \n
			message := bytes.NewBuffer(messageBuf)
			message.WriteByte(uint8(0))
			message.Write(correlationId)
			message.WriteByte(';')
			message.Write(amount)
			message.WriteByte('\n')

			_, err = unixgramConn.WriteTo(message.Bytes(), s.UnixSocketAddress)
			if err != nil {
				log.Printf("redirecting POST data: %v\n", err)
				return
			}
		} else if bytes.Equal(verb, GET) && bytes.Equal(uri, PAYMENTS_SUMMARY_ENDPOINT) {
			// This branch is severely less
			// optimized than the previous one.
			// The GET endpoint isn't
			// supposed to be under heavy
			// stress, so we allow ourselves
			// to be less performant here in
			// order to have code that is easier
			// to read. We have significantly more
			// allocations and string operations here
			// and much less raw byte manipulation.

			var responseBuf []byte
			response := bytes.NewBuffer(responseBuf)

			var from, to string
			if queryArgs != nil {
				from = queryArgs["from"]
				to = queryArgs["to"]
			}

			// formatting request to backend
			// The payload format is this:
			// <one byte>from;to\n
			// The first byte indicates whether we're
			// adding a new payment to be processed or
			// we want to retrieve a summary of payments
			// 0x0 = process new payment
			// 0x1 = get summary
			messageBuf := make([]byte, 0, len(from)+len(to)+3) // 3 = first byte + ; + \n
			message := bytes.NewBuffer(messageBuf)
			message.WriteByte(uint8(1))
			message.WriteString(from)
			message.WriteByte(';')
			message.WriteString(to)
			message.WriteByte('\n')

			_, err := unixgramConn.WriteTo(message.Bytes(), s.UnixSocketAddress)
			if err != nil {
				log.Printf("err: %v\n", err)
				response.Write(HTTP_500_Internal_Server_Error)

				c.Write(response.Bytes())
				return
			}

			// reading backend response
			backendResponseBuf := make([]byte, 512)
			bytesRead, _, err := unixgramConn.ReadFromUnix(backendResponseBuf)
			if err != nil {
				log.Printf("reading from backend server: %v\n", err)
				response.Write(HTTP_500_Internal_Server_Error)

				c.Write(response.Bytes())
				return
			}

			backendResponse := backendResponseBuf[1:bytesRead]

			response.Write(HTTP_200_OK)
			response.WriteString("Content-Type: application/json\r\n")
			response.WriteString(fmt.Sprintf("Content-Length: %d\r\n", len(backendResponse)))
			response.WriteString("\r\n")
			response.Write(backendResponse)

			c.Write(response.Bytes())
		} else {
			c.Write(HTTP_404_NOT_FOUND)
		}

	})
	err = engine.Start()
	if err != nil {
		fmt.Printf("nbio.Start failed: %v\n", err)
		return
	}
	defer engine.Stop()

	fmt.Println("up and running")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	os.Remove(unixSocketPath)
	os.Exit(1)
}
