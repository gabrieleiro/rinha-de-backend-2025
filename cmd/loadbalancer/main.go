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

var HTTP_200_OK_RETURN = []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
var BODY_SEPARATOR = []byte("\r\n\r\n")
var POST = []byte("POST")

var HTTP_200_OK = []byte("HTTP/1.1 200 OK\r\n")
var HTTP_500_Internal_Server_Error = []byte("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n")

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
	address := os.Getenv("ADDRESS")

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

			s := pickServer()

			if bytes.Equal(requestData[:4], POST) {
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
			} else {
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

				lines := strings.Split(string(requestData[:]), "\r\n")
				// The first line of an HTTP request is formatted as such:
				// VERB /path/to/resource
				// In this case:
				// GET /payments-summary?from=2025-07-29T20:19:32.734Z&to=2025-07-29T20:19:45.690Z
				path := strings.Split(lines[0], " ")[1]

				var from, to string
				if len(strings.Split(path, "?")) > 1 {
					argAssignments := strings.Split(path, "?")[1]

					queryArgs := make(map[string]string)
					for _, v := range strings.Split(argAssignments, "&") {
						assignment := strings.Split(v, "=")
						queryArgs[assignment[0]] = assignment[1]
					}

					if v, ok := queryArgs["from"]; ok {
						from = v
					}

					if v, ok := queryArgs["to"]; ok {
						to = v
					}
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
			}
		}()
	}
}
