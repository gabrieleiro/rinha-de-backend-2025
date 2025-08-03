package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.design/x/lockfree"
)

var DEFAULT_PROCESSOR_URL = os.Getenv("DEFAULT_PROCESSOR_URL")
var DEFAULT_PAYMENTS_ENDPOINT = DEFAULT_PROCESSOR_URL + "/payments"
var DEFAULT_SERVICE_HEALTH_ENDPOINT = DEFAULT_PROCESSOR_URL + "/payments/service-health"

var FALLBACK_PROCESSOR_URL = os.Getenv("FALLBACK_PROCESSOR_URL")
var FALLBACK_PAYMENTS_ENDPOINT = FALLBACK_PROCESSOR_URL + "/payments"
var FALLBACK_SERVICE_HEALTH_ENDPOINT = FALLBACK_PROCESSOR_URL + "/payments/service-health"

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
	TrackerConn *net.UDPConn
}

var serverConn *net.UnixConn

var httpClient http.Client = http.Client{
	Timeout: 2000 * time.Millisecond,
}

type PaymentRequest struct {
	Amount        float64 `json:"amount"`
	CorrelationID string  `json:"correlationId"`
	RequestedAt   string  `json:"requestedAt"`
}

type ServiceHealth struct {
	Failing         bool `json:"failing"`
	MinResponseTime int  `json:"minResponseTime"`
}

type HealthChecker struct {
	Default  ServiceHealth
	Fallback ServiceHealth
}

func (hc *HealthChecker) check() {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		resp, err := http.Get(DEFAULT_SERVICE_HEALTH_ENDPOINT)
		if err != nil {
			log.Printf("checking default's health: %v\n", err)
		}

		if resp != nil {
			defer resp.Body.Close()
		}

		json.NewDecoder(resp.Body).Decode(&hc.Default)
	}()

	go func() {
		defer wg.Done()
		resp, err := http.Get(FALLBACK_SERVICE_HEALTH_ENDPOINT)
		if err != nil {
			log.Printf("checking fallback's health: %v\n", err)
			return
		}

		if resp != nil {
			defer resp.Body.Close()
		}

		json.NewDecoder(resp.Body).Decode(&hc.Fallback)
	}()

	wg.Wait()
}

var healthChecker HealthChecker

func tryPay(endpoint string, pr PaymentRequest) error {
	payload, err := json.Marshal(pr)
	if err != nil {
		log.Printf("encoding json: %v\n", err)
		return err
	}

	response, err := httpClient.Post(endpoint, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		// If the error is not on their end,
		// we just give up on processing the
		// request. The two most probable
		// causes are:
		// 1. This correlationId has already been processed
		// 2. We made a mistake when building the payload
		//
		// There are clever ways to handle
		// these cases, like having a reconciliation
		// function that tracks already
		// processsed payments. I don't
		// have any more time to spend
		// on this project though :/
		if response.StatusCode != http.StatusInternalServerError {
			return nil
		}

		return errors.New(response.Status)
	}

	return nil
}

func tryProcessing(pr PaymentRequest) error {
	if healthChecker.Default.Failing && healthChecker.Fallback.Failing {
		return errors.New("both processors are down")
	}

	if healthChecker.Default.MinResponseTime > MAX_TIMEOUT_IN_MS && healthChecker.Fallback.MinResponseTime > MAX_TIMEOUT_IN_MS {
		return errors.New("both processors are slow")
	}

	if !healthChecker.Default.Failing && healthChecker.Default.MinResponseTime < MAX_TIMEOUT_IN_MS {
		now := time.Now().UTC()
		pr.RequestedAt = now.UTC().Format(time.RFC3339Nano)
		err := tryPay(DEFAULT_PAYMENTS_ENDPOINT, pr)

		if err == nil {
			trackPayment(pr, "default")
			return nil
		}
	}

	if !healthChecker.Fallback.Failing && healthChecker.Default.MinResponseTime < MAX_TIMEOUT_IN_MS {
		now := time.Now().UTC()
		pr.RequestedAt = now.UTC().Format(time.RFC3339Nano)
		err := tryPay(FALLBACK_PAYMENTS_ENDPOINT, pr)

		if err == nil {
			trackPayment(pr, "fallback")
			return nil
		} else {
			return err
		}
	}

	return errors.New("undefined error")
}

func trackPayment(pr PaymentRequest, processor string) {
	var message strings.Builder
	message.WriteByte(0)
	message.WriteString(fmt.Sprintf("%s;%f;%s\n", processor, pr.Amount, pr.RequestedAt))
	_, err := udpClient.TrackerConn.Write([]byte(message.String()))
	if err != nil {
		log.Printf("sending message to tracker: %v\n", err)
		return
	}
}

type Request struct {
	w http.ResponseWriter
	r *http.Request
}

func paymentsSummary(addr *net.UnixAddr, payload []byte) {
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

	serverConn.WriteToUnix(trackerResponse, addr)
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

	// networking
	var err error
	udpClient.TrackerConn, err = dialUDP(TRACKER_URL)
	if err != nil {
		log.Printf("initing udp client: %v\n", err)
		return
	}
	defer udpClient.TrackerConn.Close()

	unixAddr, err := net.ResolveUnixAddr("unixgram", serverAddress)
	if err != nil {
		log.Printf("resolving address: %v\n", err)
		return
	}

	serverConn, err = net.ListenUnixgram("unixgram", unixAddr)
	if err != nil {
		log.Printf("listening unix socket: %v\n", err)
		return
	}

	// init queues
	mainQueue := lockfree.NewQueue()
	retryQueue := lockfree.NewQueue()

	go func() {
		for {
			pr := mainQueue.Dequeue()
			if pr == nil {
				continue
			}

			err := tryProcessing(pr.(PaymentRequest))
			if err != nil {
				retryQueue.Enqueue(pr)
			}
		}
	}()

	go func() {
		for {
			pr := retryQueue.Dequeue()
			if pr == nil {
				continue
			}

			err := tryProcessing(pr.(PaymentRequest))
			if err != nil {
				retryQueue.Enqueue(pr)
			}
		}
	}()

	fmt.Println("up and running")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		os.Remove(serverAddress)
		os.Exit(1)
	}()

	// listen to messages from load balancer
	for {
		var buf [512]byte

		n, addr, err := serverConn.ReadFromUnix(buf[:])
		if err != nil {
			log.Printf("reading from connection: %v\n", err)
			continue
		}

		go func() {
			data := buf[:n-1]

			if data[0] == 0 { // POST
				params := strings.Split(string(data[1:]), ";")
				if len(params) != 2 || params[0] == "" || params[1] == "" {
					log.Printf("malformed message: %v\n", string(data[1:]))
					return
				}

				amount, err := strconv.ParseFloat(params[1], 64)
				if err != nil {
					log.Printf("parsing float: %v\n", err)
					return
				}

				mainQueue.Enqueue(PaymentRequest{amount, params[0], ""})
			} else { // GET
				go paymentsSummary(addr, data)
			}
		}()
	}

}
