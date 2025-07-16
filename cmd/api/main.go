package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

const MAX_SIZE_OF_TCP_PACKET = 65535
const TCP_CONNECTION_POOL_SIZE = 200

const HTTP_OK = "HTTP/1.1 200 OK\r\n\r\n"
const HTTP_INTERNAL_SERVER_ERROR = "HTTP/1.1 500 Internal Server Error\r\n"
const HTTP_NOT_FOUND = "HTTP/1.1 404 Not Found\r\n\r\n"

var DEFAULT_PROCESSOR_URL = os.Getenv("DEFAULT_PROCESSOR_URL")
var DEFAULT_PAYMENTS_ENDPOINT = DEFAULT_PROCESSOR_URL + "/payments"
var DEFAULT_SERVICE_HEALTH_ENDPOINT = DEFAULT_PROCESSOR_URL + "/payments/service-health"

var FALLBACK_PROCESSOR_URL = os.Getenv("FALLBACK_PROCESSOR_URL")
var FALLBACK_PAYMENTS_ENDPOINT = FALLBACK_PROCESSOR_URL + "/payments"
var FALLBACK_SERVICE_HEALTH_ENDPOINT = FALLBACK_PROCESSOR_URL + "/payments/service-health"

var TRACKER_URL = os.Getenv("TRACKER_URL")
var PAYMENTS_SUMMARY_ENDPOINT = TRACKER_URL + "/summary"
var TRACK_PAYMENTS_ENDPOINT = TRACKER_URL + "/track"

const HEALTH_CHECKER_INTERVAL = 6 * time.Second
const MAX_TIMEOUT_IN_MS = 200

var client http.Client = http.Client{
	Timeout: 200 * time.Millisecond,
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

var paymentWorkerChan chan PaymentRequest = make(chan PaymentRequest, 10_000)
var retriesChan chan PaymentRequest = make(chan PaymentRequest, 1000)

type Request struct {
	w http.ResponseWriter
	r *http.Request
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
	defer response.Body.Close()

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

	if healthChecker.Default.MinResponseTime > MAX_TIMEOUT_IN_MS && healthChecker.Fallback.MinResponseTime > MAX_TIMEOUT_IN_MS {
		return errors.New("both processors are slow")
	}

	if !healthChecker.Default.Failing && healthChecker.Default.MinResponseTime < MAX_TIMEOUT_IN_MS {
		now := time.Now().UTC()
		err := tryPost(DEFAULT_PAYMENTS_ENDPOINT, &pr, now)

		if err == nil {
			trackPayment(pr, "default")
			return nil
		}
	}

	if !healthChecker.Fallback.Failing && healthChecker.Default.MinResponseTime < MAX_TIMEOUT_IN_MS {
		now := time.Now().UTC()
		err := tryPost(FALLBACK_PAYMENTS_ENDPOINT, &pr, now)

		if err == nil {
			trackPayment(pr, "fallback")
			return nil
		} else {
			return err
		}
	}

	return nil
}

func parseJson(buffer []byte) (string, float64, error) {
	var amount float64
	var correlationId string

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

			correlationId = string(buffer[stringStart:i])
		} else if c == 'a' { // amount
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
			amount, err = strconv.ParseFloat(string(buffer[numberStart:i]), 64)
			if err != nil {
				return correlationId, amount, errors.New("parsing float")
			}
		}
	}

	return correlationId, amount, nil
}

func payments(ctx *fasthttp.RequestCtx) {
	correlationId, amount, err := parseJson(ctx.PostBody())
	if err != nil {
		ctx.SetStatusCode(500)
		return
	}

	go func() {
		paymentWorkerChan <- PaymentRequest{
			Amount:        amount,
			CorrelationID: correlationId,
		}
	}()

	ctx.SetStatusCode(200)
}

func paymentsSummary(ctx *fasthttp.RequestCtx) {
	from := ctx.URI().QueryArgs().PeekBytes([]byte("from"))
	to := ctx.URI().QueryArgs().PeekBytes([]byte("to"))
	// fromString := r.URL.Query().Get("from")
	// toString := r.URL.Query().Get("to")

	message := fmt.Sprintf("s%s;%s\n", string(from), string(to))

	_, err := udpClient.Conn.Write([]byte(message))
	if err != nil {
		log.Printf("sending message to tracker: %v\n", err)
		return
	}

	trackerResponse, err := bufio.NewReader(udpClient.Conn).ReadString('\n')
	if err != nil {
		log.Printf("reading from tracker: %v\n", err)

		ctx.SetStatusCode(500)
		return
	}

	data := strings.Split(trackerResponse[:len(trackerResponse)-1], ";")
	defaultRequests, err := strconv.ParseInt(data[0], 10, 64)
	if err != nil {
		log.Printf("parsing integer: %v\n", err)
		ctx.SetStatusCode(500)
		return
	}
	defaultAmount, err := strconv.ParseFloat(data[1], 64)
	if err != nil {
		log.Printf("parsing float: %v\n", err)

		ctx.SetStatusCode(500)
		return
	}

	fallbackRequests, err := strconv.ParseInt(data[2], 10, 64)
	if err != nil {
		log.Printf("parsing integer: %v\n", err)

		ctx.SetStatusCode(500)
		return
	}
	fallbackAmount, err := strconv.ParseFloat(data[3], 64)
	if err != nil {
		log.Printf("parsing float: %v\n", err)

		ctx.SetStatusCode(500)
		return
	}

	j := fmt.Sprintf(`{
	"default": {
		"totalRequests": %d,
		"totalAmount": %f
	},
	"fallback": {
		"totalRequests": %d,
		"totalAmount": %f
	}
}`, int(defaultRequests), defaultAmount, int(fallbackRequests), fallbackAmount)

	ctx.WriteString(j)
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

func requestHandler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/payments":
		payments(ctx)
	case "/payments-summary":
		paymentsSummary(ctx)
	}
}

func main() {
	serverAddress := os.Getenv("ADDRESS")

	udpAddr, err := net.ResolveUDPAddr("udp", TRACKER_URL)
	if err != nil {
		log.Printf("resolving payments tracker service address: %v\n", err)
		return
	}

	udpClient.Conn, err = net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		log.Printf("establishing udp connection with payments tracker: %v\n", err)
		return
	}
	defer udpClient.Conn.Close()

	for i := 0; i < 1000; i++ {
		go func() {
			for pr := range paymentWorkerChan {
				err := tryProcessing(pr)
				// retry
				if err != nil {
					retriesChan <- pr
				}
			}
		}()
	}

	// http.HandleFunc("/payments", payments)
	// http.HandleFunc("/payments-summary", paymentsSummary)

	fasthttp.ListenAndServe(serverAddress, requestHandler)

	for i := 0; i < 100; i++ {
		go func() {
			for retry := range retriesChan {
				err := tryProcessing(retry)
				if err != nil {
					retriesChan <- retry
				}
			}
		}()
	}

	go func() {
		ticker := time.NewTicker(HEALTH_CHECKER_INTERVAL)

		for range ticker.C {
			healthChecker.check()
		}
	}()
}
