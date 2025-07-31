package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"unicode"

	"log"
	"math/rand"
	"net"
	"os"
	"strings"
)

var AMOUNT []byte

var POST = []byte("POST")
var GET = []byte("GET")

var HTTP_200_OK = []byte("HTTP/1.1 200 OK\r\n")
var HTTP_200_OK_RETURN = []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
var HTTP_500_Internal_Server_Error = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n")
var HTTP_404_NOT_FOUND = []byte("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n")

var BODY_SEPARATOR = []byte("\r\n\r\n")

var PAYMENTS_ENDPOINT = []byte("/payments")
var PAYMENTS_SUMMARY_ENDPOINT = []byte("/payments-summary")

var SERVERS = []ServerInfo{}

type ServerInfo struct {
	Address string
	Conn    *net.UDPConn
}

func pickServer() *ServerInfo {
	// I mean... it's just two servers anyway
	return &SERVERS[rand.Intn(len(SERVERS))]
}

// zero-allocation json parser
// only parses "correlationId" and "amount" fields
func parseJson(buffer []byte) ([]byte, []byte, error) {
	amount := AMOUNT
	var correlationId []byte
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
			// amount is always the same in all requests
			// so if we only parse the first occurance
			// and reuse that
			if amount != nil {
				break
			}

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
			AMOUNT = buffer[numberStart:pos]

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

	address := os.Getenv("ADDRESS")
	if address == "" {
		address = ":9999"
	}

	// set up connection to servers
	for _, sa := range serverAddresses {
		si := ServerInfo{Address: sa}
		udpAddr, err := net.ResolveUDPAddr("udp", sa)
		if err != nil {
			log.Printf("resolving udp address: %v\n", sa)
			return
		}

		si.Conn, err = net.DialUDP("udp", nil, udpAddr)
		if err != nil {
			log.Printf("dialing udp server: %v\n", err)
			return
		}

		SERVERS = append(SERVERS, si)
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Printf("listening on %s: %v\n", address, err)
		return
	}

	fmt.Println("up and running")

	for {
		src, err := listener.Accept()
		if err != nil {
			log.Printf("accepting tcp connection: %v\n", err)
			continue
		}

		go func() {
			defer src.Close()

			requestData := make([]byte, 512)
			_, err := src.Read(requestData)
			if err != nil {
				log.Printf("reading request: %v\n", err)
				return
			}

			verb, uri, queryArgs, err := parseRequestLine(requestData)
			if err != nil {
				log.Printf("parsing request line: %v\n", err)
				return
			}

			s := pickServer()

			if bytes.Equal(verb, POST) && bytes.Equal(uri, PAYMENTS_ENDPOINT) {
				// POST doesn't return anything
				// and is processed assynchronously
				// so we just return early
				_, err = src.Write(HTTP_200_OK_RETURN)
				if err != nil {
					log.Printf("writing response: %v\n", err)
					return
				}

				src.Close()

				correlationId, amount, err := parseJson(bodyFrom(requestData))
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

				_, err = s.Conn.Write(message.Bytes())
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

				_, err := s.Conn.Write(message.Bytes())
				if err != nil {
					log.Printf("err: %v\n", err)
					response.Write(HTTP_500_Internal_Server_Error)

					src.Write(response.Bytes())
					return
				}

				// reading backend response
				backendResponse, err := bufio.NewReader(s.Conn).ReadBytes('\n')
				if err != nil {
					log.Printf("reading from backend server: %v\n", err)
					response.Write(HTTP_500_Internal_Server_Error)

					src.Write(response.Bytes())
					return
				}

				if err != nil {
					log.Printf("parsing summary to json: %v\n", err)
					response.Write(HTTP_500_Internal_Server_Error)

					src.Write(response.Bytes())
					return
				}

				response.Write(HTTP_200_OK)
				response.WriteString("Content-Type: application/json\r\n")
				response.WriteString(fmt.Sprintf("Content-Length: %d\r\n", len(backendResponse)))
				response.WriteString("\r\n")
				response.Write(backendResponse)

				src.Write(response.Bytes())
			} else {
				src.Write(HTTP_404_NOT_FOUND)
			}
		}()
	}
}
