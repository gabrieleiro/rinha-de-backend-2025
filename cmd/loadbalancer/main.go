package main

import (
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
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

			s := pickServer()
			dst, err := net.Dial("tcp", s.Address)
			if err != nil {
				log.Printf("dialing server %s: %v\n", s, err)
				return
			}

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				io.Copy(dst, src)
			}()

			go func() {
				defer wg.Done()
				io.Copy(src, dst)
			}()

			wg.Wait()
			s.RecycleConnection(dst)
		}()
	}
}
