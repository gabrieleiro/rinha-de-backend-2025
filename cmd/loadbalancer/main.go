package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
)

var SERVERS = []ServerInfo{}

type ServerInfo struct {
	Connection *httputil.ReverseProxy
}

func pickServer() ServerInfo {
	return SERVERS[rand.Intn(len(SERVERS))]
}

func main() {
	servers := strings.Split(os.Getenv("SERVERS"), ",")
	address := os.Getenv("ADDRESS")

	for _, s := range servers {
		serverUrl, err := url.Parse(s)
		if err != nil {
			log.Printf("parsing url: %v\n", err)
			return
		}

		proxy := httputil.NewSingleHostReverseProxy(serverUrl)
		proxy.Transport = &http.Transport{
			MaxIdleConnsPerHost: 200,
		}

		SERVERS = append(SERVERS, ServerInfo{
			Connection: proxy,
		})
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		s := pickServer()
		s.Connection.ServeHTTP(w, r)
	})

	err := http.ListenAndServe(address, nil)
	if err != nil {
		log.Printf("listening on address %s: %v\n", address, err)
	}
}
