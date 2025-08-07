package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type MessageType uint8

const (
	ProcessPayment MessageType = iota
	RetrieveSummary
	Summary
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
const MAX_TIMEOUT_IN_MS = 150

var mainQueue chan *PaymentRequest
var retryQueue chan *PaymentRequest

var prPool = sync.Pool{
	New: func() any {
		return &PaymentRequest{}
	},
}

var zeroPr = &PaymentRequest{}

func clearPr(pr *PaymentRequest) {
	*pr = *zeroPr
}

var serverConn *net.UnixConn
var trackerAddr *net.UnixAddr
var lbAddr *net.UnixAddr

var client http.Client = http.Client{
	Timeout: 150 * time.Millisecond,
}

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

func tryPay(endpoint string, pr *PaymentRequest) error {
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

	io.Copy(io.Discard, response.Body)

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

func tryProcessing(pr *PaymentRequest) error {
	defaultTime := healthChecker.Default.MinResponseTime
	fallbackTime := healthChecker.Default.MinResponseTime

	defaultGood := !healthChecker.Default.Failing && defaultTime < MAX_TIMEOUT_IN_MS
	fallbackGood := !healthChecker.Fallback.Failing && fallbackTime < MAX_TIMEOUT_IN_MS

	if defaultGood {
		now := time.Now().UTC()
		pr.RequestedAt = now.UTC().Format(time.RFC3339Nano)
		err := tryPay(DEFAULT_PAYMENTS_ENDPOINT, pr)

		if err == nil {
			trackPayment(pr, "default")
		}

		return err
	} else if fallbackGood {
		now := time.Now().UTC()
		pr.RequestedAt = now.UTC().Format(time.RFC3339Nano)
		err := tryPay(FALLBACK_PAYMENTS_ENDPOINT, pr)

		if err == nil {
			trackPayment(pr, "fallback")
		}

		return err
	} else {
		time.Sleep(time.Duration(min(defaultTime, fallbackTime)) * time.Millisecond)
		return errors.New("both processors are bad")
	}
}

func trackPayment(pr *PaymentRequest, processor string) {
	var message strings.Builder
	message.WriteByte(0)
	message.WriteString(fmt.Sprintf("%s;%f;%s\n", processor, pr.Amount, pr.RequestedAt))
	_, err := serverConn.WriteTo([]byte(message.String()), trackerAddr)
	if err != nil {
		log.Printf("sending message to tracker: %v\n", err)
		return
	}
}

func paymentsSummary(payload []byte) {
	_, err := serverConn.WriteTo(append(payload, '\n'), trackerAddr)
	if err != nil {
		log.Printf("sending message to tracker: %v\n", err)
		return
	}
}

func paymentRequest(payload []byte) {
	params := strings.Split(string(payload), ";")
	if len(params) != 2 || params[0] == "" || params[1] == "" {
		log.Printf("malformed message: %v\n", string(payload))
		return
	}

	amount, err := strconv.ParseFloat(params[1], 64)
	if err != nil {
		log.Printf("parsing float: %v\n", err)
		return
	}

	newPr := prPool.Get().(*PaymentRequest)
	newPr.Amount = amount
	newPr.CorrelationID = params[0]

	mainQueue <- newPr
}

func main() {
	runtime.GOMAXPROCS(1)
	serverAddress := os.Getenv("ADDRESS")
	os.Remove(serverAddress)

	// networking

	loadBalancerURI := os.Getenv("LOAD_BALANCER_URI")
	if loadBalancerURI == "" {
		loadBalancerURI = "./sockets/rinha_load_balancer.sock"
	}

	var trackerURI = os.Getenv("TRACKER_URI")
	if trackerURI == "" {
		trackerURI = "./sockets/rinha_tracker.sock"
	}

	var err error
	trackerAddr, err = net.ResolveUnixAddr("unixgram", trackerURI)
	if err != nil {
		log.Printf("resolving tracker URI: %v\n", err)
		return
	}

	lbAddr, err = net.ResolveUnixAddr("unixgram", loadBalancerURI)
	if err != nil {
		log.Printf("resolving load balancer URI: %v\n", err)
		return
	}

	serverAddr, err := net.ResolveUnixAddr("unixgram", serverAddress)
	if err != nil {
		log.Printf("resolving server URI: %v\n", err)
		return
	}

	serverConn, err = net.ListenUnixgram("unixgram", serverAddr)
	if err != nil {
		log.Printf("listening on socket: %v\n", err)
		return
	}

	// init queues
	mainQueue = make(chan *PaymentRequest)
	retryQueue = make(chan *PaymentRequest, 5_000)

	// consume main queue
	for range 3 {
		go func() {
			for pr := range mainQueue {
				err := tryProcessing(pr)

				if err != nil {
					time.Sleep(time.Duration(50) * time.Millisecond)
					retryQueue <- pr
				} else {
					prPool.Put(pr)
				}
			}
		}()
	}

	// consume retry queue
	go func() {
		for pr := range retryQueue {
			err := tryProcessing(pr)
			if err != nil {
				time.Sleep(time.Duration(50) * time.Millisecond)
				retryQueue <- pr
			} else {
				prPool.Put(pr)
			}
		}
	}()

	// listen to messages from load balancer
	go func() {
		for {
			var buf [512]byte

			n, _, err := serverConn.ReadFromUnix(buf[:])
			if err != nil {
				log.Printf("reading from connection: %v\n", err)
				continue
			}

			data := buf[:n-1]

			switch MessageType(data[0]) {
			case ProcessPayment:
				go paymentRequest(data[1:])
			case RetrieveSummary:
				go paymentsSummary(data)
			case Summary:
				go serverConn.WriteTo(data, lbAddr)
			default:
				log.Printf("unrecognized message: %v\n", string(data))
			}
		}
	}()

	fmt.Println("up and running")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	os.Remove(serverAddress)
	os.Exit(1)
}
