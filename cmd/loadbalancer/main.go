package main

import (
	"bytes"
	"sync"

	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
)

const MAX_CONNECTIONS_PER_SERVER = 200

var SERVERS = []ServerInfo{}

type ServerInfo struct {
	Address string
	Pool    chan net.Conn
}

func (si *ServerInfo) GetConnection() (net.Conn, error) {
	for {
		select {
		case conn := <-si.Pool:
			return conn, nil

		default:
			conn, err := net.Dial("tcp", si.Address)
			if err != nil {
				return nil, err
			}

			return conn, nil
		}
	}
}

func (si *ServerInfo) RecycleConnection(conn net.Conn) {
	select {
	case si.Pool <- conn:
	default:
		conn.Close()
	}
}

func InitConnectionPool(s *ServerInfo) {
	s.Pool = make(chan net.Conn, MAX_CONNECTIONS_PER_SERVER)
}

func pickServer() *ServerInfo {
	return &SERVERS[rand.Intn(len(SERVERS))]
}

func main() {
	serverAddresses := strings.Split(os.Getenv("SERVERS"), ",")
	address := os.Getenv("ADDRESS")

	for _, sa := range serverAddresses {
		si := ServerInfo{Address: sa}
		InitConnectionPool(&si)
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
			dataLength, err := src.Read(requestData)
			if err != nil {
				log.Printf("reading request: %v\n", err)
			}

			s := pickServer()

			dst, err := s.GetConnection()
			if err != nil {
				log.Printf("dialing server %s: %v\n", s, err)
				return
			}

			defer s.RecycleConnection(dst)

			if bytes.Equal(requestData[:4], []byte("POST")) {
				if err != nil {
					log.Printf("reading POST data: %v\n", err)
					return
				}

				response := []byte("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
				_, err = src.Write(response)
				if err != nil {
					log.Printf("writing response: %v\n", err)
				}

				_, err = dst.Write(requestData[:dataLength])
				if err != nil {
					log.Printf("redirecting POST data: %v\n", err)
					return
				}
			} else {
				var wg sync.WaitGroup
				wg.Add(2)

				go func() {
					defer wg.Done()
					_, err := dst.Write(requestData[:dataLength])
					if err != nil {
						log.Printf("err: %v\n", err)
					}
				}()

				go func() {
					defer wg.Done()
					_, err := io.Copy(src, dst)
					if err != nil {
						log.Printf("err: %v\n", err)
					}
				}()

				wg.Wait()
			}
		}()
	}
}
