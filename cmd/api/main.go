package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"
)

var TRACKER_URL = os.Getenv("TRACKER_URL")
var PAYMENTS_SUMMARY_ENDPOINT = TRACKER_URL + "/summary"
var TRACK_PAYMENTS_ENDPOINT = TRACKER_URL + "/track"

var PAYMENT_QUEUE_URL = os.Getenv("PAYMENT_QUEUE_URL")

const HEALTH_CHECKER_INTERVAL = 6 * time.Second
const MAX_TIMEOUT_IN_MS = 200

var client http.Client = http.Client{
	Timeout: 200 * time.Millisecond,
}

var udpClient struct {
	QueueConn   *net.UDPConn
	TrackerConn *net.UDPConn
}

var udpServer struct {
	Conn *net.UDPConn
}

type PaymentRequest struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

func (pr PaymentRequest) String() string {
	return fmt.Sprintf("%s\n%d", pr.CorrelationID, pr.Amount)
}

var paymentWorkerChan chan PaymentRequest = make(chan PaymentRequest, 10_000)
var retriesChan chan PaymentRequest = make(chan PaymentRequest, 1000)

type Request struct {
	w http.ResponseWriter
	r *http.Request
}

func payments(correlationId string, amount float64) {
	message := fmt.Sprintf("%s;%f\n", correlationId, amount)
	_, err := udpClient.QueueConn.Write([]byte(message))
	if err != nil {
		log.Printf("sending message to payment queue: %v\n", err)
		return
	}
}

func paymentsSummary(addr *net.UDPAddr, payload []byte) {
	_, err := udpClient.TrackerConn.Write(append(payload, '\n'))
	if err != nil {
		log.Printf("sending message to tracker: %v\n", err)
		return
	}

	trackerResponse, err := bufio.NewReader(udpClient.TrackerConn).ReadBytes('\n')
	if err != nil {
		log.Printf("reading from tracker: %v\n", err)
		return
	}

	udpServer.Conn.WriteToUDP(trackerResponse, addr)
}

func dialUDP(addressString string) (*net.UDPConn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addressString)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func main() {
	serverAddress := os.Getenv("ADDRESS")

	var err error
	udpClient.TrackerConn, err = dialUDP(TRACKER_URL)
	if err != nil {
		log.Printf("initing udp client: %v\n", err)
		return
	}
	defer udpClient.TrackerConn.Close()

	udpClient.QueueConn, err = dialUDP(PAYMENT_QUEUE_URL)
	if err != nil {
		log.Printf("initing udp client: %v\n", err)
		return
	}
	defer udpClient.QueueConn.Close()

	udpAddr, err := net.ResolveUDPAddr("udp", serverAddress)
	if err != nil {
		log.Printf("resolving address: %v\n", err)
		return
	}

	udpServer.Conn, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Printf("listening on UDP port: %v\n", err)
		return
	}

	for {
		var buf [512]byte

		n, addr, err := udpServer.Conn.ReadFromUDP(buf[0:])
		if err != nil {
			log.Printf("reading from connection: %v\n", err)
			continue
		}

		go func() {
			data := buf[:n-1]

			if data[0] == 0 { // POST
				params := strings.Split(string(data[1:]), ";")
				amount, err := strconv.ParseFloat(params[1], 64)
				if err != nil {
					log.Printf("parsing float: %v\n", err)
					return
				}

				payments(params[0], amount)
			} else { // GET
				paymentsSummary(addr, data)
			}
		}()
	}
}
