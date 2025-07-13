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
	"os"
	"strconv"
	"strings"
	"time"
)

const DEFAULT_PAYMENTS_ENDPOINT = "http://127.0.0.1:8001/payments"
const FALLBACK_PAYMENTS_ENDPOINT = "http://127.0.0.1:8002/payments"
const PAYMENTS_SUMMARY_ENDPOINT = "http://localhost:1111/summary"
const TRACK_PAYMENTS_ENDPOINT = "http://localhost:1111/track"

const DEFAULT_SERVICE_HEALTH_ENDPOINT = "http://127.0.0.1:8001/payments/service-health"
const FALLBACK_SERVICE_HEALTH_ENDPOINT = "http://127.0.0.1:8002/payments/service-health"

const HEALTH_CHECKER_INTERVAL = 6 * time.Second
const MAX_TIMEOUT_IN_MS = 200

var client http.Client = http.Client{
	// Timeout: 200 * time.Millisecond,
}

var udpClient struct {
	Conn *net.UDPConn
}

type PaymentRequest struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

type CombinedPaymentsSummary struct {
	Default  PaymentsSummary `json:"default"`
	Fallback PaymentsSummary `json:"fallback"`
}

type AmountWithTime struct {
	Amount float64
	Time   time.Time
}

type PaymentsSummary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

func (pr PaymentRequest) String() string {
	return fmt.Sprintf("%s\n%d", pr.CorrelationID, pr.Amount)
}

type RetryQueue struct {
	C chan Retry
}

type Request struct {
	w http.ResponseWriter
	r *http.Request
}

type Retry struct {
	Pr PaymentRequest
}

func (rq *RetryQueue) Push(pr PaymentRequest) {
	rq.C <- Retry{Pr: pr}
}

func (rq *RetryQueue) Consume() {
	r := <-rq.C
	tryProcessing(r.Pr)
}

var retryQueue = RetryQueue{
	C: make(chan Retry),
}

func tryPost(endpoint string, pr *PaymentRequest, now time.Time) error {
	pr.RequestedAt = now.Format(time.RFC3339Nano)

	payload, err := json.Marshal(pr)
	if err != nil {
		log.Printf("encoding json: %v\n", err)
		return err
	}

	response, err := client.Post(endpoint, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	if response.StatusCode != http.StatusOK {
		return errors.New(response.Status)
	}

	return nil
}

type TrackRequest struct {
	Processor string  `json:"processor"`
	Amount    float64 `json:"amount"`
	Time      string  `json:"time"`
}

func trackPayment(pr PaymentRequest, processor string) {
	message := fmt.Sprintf("t%s;%f;%s\n", processor, pr.Amount, pr.RequestedAt)

	_, err := udpClient.Conn.Write([]byte(message))
	if err != nil {
		log.Printf("sending message to tracker: %v\n", err)
		return
	}
}

func tryProcessing(pr PaymentRequest) error {
	if healthChecker.Default.Failing && healthChecker.Fallback.Failing {
		return errors.New("both processors are down")
	}

	if !healthChecker.Default.Failing && healthChecker.Default.MinResponseTime < MAX_TIMEOUT_IN_MS {
		now := time.Now().UTC()
		err := tryPost(DEFAULT_PAYMENTS_ENDPOINT, &pr, now)

		if err == nil {
			go trackPayment(pr, "default")
			return nil
		}
	}

	if !healthChecker.Fallback.Failing {
		now := time.Now().UTC()
		err := tryPost(FALLBACK_PAYMENTS_ENDPOINT, &pr, now)

		if err == nil {
			go trackPayment(pr, "fallback")
			return nil
		} else {
			return err
		}
	}

	return nil
}

func payments(w http.ResponseWriter, r *http.Request) {
	begin := time.Now()

	var pr PaymentRequest

	err := json.NewDecoder(r.Body).Decode(&pr)
	if err != nil {
		log.Printf("decoding json: %v\n", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	go func() {
		err = tryProcessing(pr)
		if err != nil {
			retryQueue.Push(pr)
		}
	}()

	elapsed := time.Since(begin)

	if elapsed > 2*time.Millisecond {
		log.Printf("slow payments: %v\n", elapsed)
	}

	return
}

func paymentsSummary(w http.ResponseWriter, r *http.Request) {
	begin := time.Now()

	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")

	message := fmt.Sprintf("s%s;%s\n", from, to)

	_, err := udpClient.Conn.Write([]byte(message))
	if err != nil {
		log.Printf("sending message to tracker: %v\n", err)
		return
	}

	response, err := bufio.NewReader(udpClient.Conn).ReadString('\n')
	if err != nil {
		log.Printf("reading from tracker: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	data := strings.Split(response[:len(response)-1], ";")
	defaultRequests, err := strconv.ParseInt(data[0], 10, 64)
	if err != nil {
		log.Printf("parsing integer: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defaultAmount, err := strconv.ParseFloat(data[1], 64)
	if err != nil {
		log.Printf("parsing float: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	fallbackRequests, err := strconv.ParseInt(data[2], 10, 64)
	if err != nil {
		log.Printf("parsing integer: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	fallbackAmount, err := strconv.ParseFloat(data[3], 64)
	if err != nil {
		log.Printf("parsing float: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = json.NewEncoder(w).Encode(CombinedPaymentsSummary{
		Default:  PaymentsSummary{int(defaultRequests), defaultAmount},
		Fallback: PaymentsSummary{int(fallbackRequests), fallbackAmount},
	})
	if err != nil {
		log.Printf("encoding json: %v\n", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	elapsed := time.Since(begin)

	if elapsed > 2*time.Millisecond {
		log.Printf("slow summary: %v\n", elapsed)
	}
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
	resp, err := http.Get(DEFAULT_SERVICE_HEALTH_ENDPOINT)
	if err != nil {
		log.Printf("checking default's health: %v", err)
	} else {
		json.NewDecoder(resp.Body).Decode(&hc.Default)
	}

	resp, err = http.Get(FALLBACK_SERVICE_HEALTH_ENDPOINT)
	if err != nil {
		log.Printf("checking fallback's health: %v", err)
	} else {
		json.NewDecoder(resp.Body).Decode(&hc.Fallback)
	}
}

var healthChecker HealthChecker

func main() {
	serverAddress := os.Getenv("ADDRESS")

	udpAddr, err := net.ResolveUDPAddr("udp", ":1111")
	if err != nil {
		log.Printf("resolving payments tracker service address: %v\n", err)
		return
	}

	udpClient.Conn, err = net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		log.Printf("establishing udp connection with payments tracker: %v\n", err)
		return
	}

	http.HandleFunc("/payments", payments)
	http.HandleFunc("/payments-summary", paymentsSummary)

	go func() {
		for {
			retryQueue.Consume()
		}
	}()

	go func() {
		ticker := time.NewTicker(HEALTH_CHECKER_INTERVAL)

		for range ticker.C {
			healthChecker.check()
		}
	}()

	http.ListenAndServe(serverAddress, nil)
}
