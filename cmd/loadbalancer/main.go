package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"
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

const HTTP_200_OK = "HTTP/1.1 200 OK\r\n"
const HTTP_500_Internal_Server_Error = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n"

var SERVERS = []ServerInfo{}

type ServerInfo struct {
	Address string
	Conn    *net.UDPConn
}

func pickServer() *ServerInfo {
	return &SERVERS[rand.Intn(len(SERVERS))]
}

func parseJson(buffer []byte) ([]byte, []byte, error) {
	amount := AMOUNT
	var correlationId []byte

	for i := 0; i < len(buffer); i++ {
		c := rune(buffer[i])
		for c == ' ' || c == '\n' || c == '\t' {
			i++
			c = rune(buffer[i])
		}

		if c != '"' {
			continue
		}

		i++
		c = rune(buffer[i])

		if c == 'c' { // correlationId
			for c != ':' {
				i++
				c = rune(buffer[i])
			}

			for c != '"' {
				i++
				c = rune(buffer[i])
			}

			i++ // skip opening "
			c = rune(buffer[i])

			stringStart := i

			for c != '"' {
				i++
				c = rune(buffer[i])
			}

			correlationId = buffer[stringStart:i]
		} else if c == 'a' { // amount
			if amount != nil {
				break
			}

			for c != ':' {
				i++
				c = rune(buffer[i])
			}

			for !unicode.IsDigit(c) {
				i++
				c = rune(buffer[i])
			}

			numberStart := i

			for unicode.IsDigit(c) {
				i++
				c = rune(buffer[i])
			}

			if c == '.' {
				i++
				c = rune(buffer[i])
			}

			for unicode.IsDigit(c) {
				i++
				c = rune(buffer[i])
			}

			var err error
			amount = buffer[numberStart:i]
			AMOUNT = buffer[numberStart:i]

			if err != nil {
				return correlationId, amount, errors.New("parsing float")
			}
		}
	}

	return correlationId, amount, nil
}

type Timings struct {
	name    string
	slowest time.Duration
	mu      sync.Mutex
}

func (t *Timings) Add(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if d > t.slowest {
		log.Printf("new slowest for %s: %v\n", t.name, d)
		t.slowest = d
	}
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

	postTimings := Timings{name: "post"}
	getTimings := Timings{name: "get"}

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
			start := time.Now()
			defer src.Close()

			requestData := make([]byte, 512)
			_, err := src.Read(requestData)
			if err != nil {
				log.Printf("reading request: %v\n", err)
				return
			}

			s := pickServer()

			if bytes.Equal(requestData[:4], POST) {
				_, err = src.Write(HTTP_200_OK_RETURN)
				if err != nil {
					log.Printf("writing response: %v\n", err)
					return
				}

				src.Close()
				postTimings.Add(time.Since(start))

				correlationId, amount, err := parseJson(bodyFrom(requestData))
				if err != nil {
					log.Printf("parsing json: %v\n", err)
					return
				}

				messageBuf := make([]byte, 0, len(correlationId)+len(amount)+3)
				message := bytes.NewBuffer(messageBuf)
				message.WriteByte(0)
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
				// parse url and query args
				var response strings.Builder

				lines := strings.Split(string(requestData[:]), "\r\n")
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
				var message strings.Builder
				message.WriteByte(1)
				message.WriteString(fmt.Sprintf("%s;%s\n", from, to))

				_, err := s.Conn.Write([]byte(message.String()))
				if err != nil {
					log.Printf("err: %v\n", err)
					response.WriteString(HTTP_500_Internal_Server_Error)

					src.Write([]byte(response.String()))
					return
				}

				// reading backend response
				backendResponse, err := bufio.NewReader(s.Conn).ReadBytes('\n')
				if err != nil {
					log.Printf("reading from backend server: %v\n", err)
					response.WriteString(HTTP_500_Internal_Server_Error)

					src.Write([]byte(response.String()))
					return
				}

				if err != nil {
					log.Printf("parsing summary to json: %v\n", err)
					response.WriteString(HTTP_500_Internal_Server_Error)

					src.Write([]byte(response.String()))
					return
				}

				response.WriteString(HTTP_200_OK)
				response.WriteString("Content-Type: application/json\r\n")
				response.WriteString(fmt.Sprintf("Content-Length: %d\r\n", len(backendResponse)))
				response.WriteString("\r\n")
				response.WriteString(string(backendResponse))

				src.Write([]byte(response.String()))
				getTimings.Add(time.Since(start))
			}
		}()
	}
}
