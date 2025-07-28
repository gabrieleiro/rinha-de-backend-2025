package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/valyala/fasthttp"
)

var AMOUNT float64 = 0

const MAX_SIZE_OF_TCP_PACKET = 65535
const TCP_CONNECTION_POOL_SIZE = 200

const HTTP_OK = "HTTP/1.1 200 OK\r\n\r\n"
const HTTP_INTERNAL_SERVER_ERROR = "HTTP/1.1 500 Internal Server Error\r\n"
const HTTP_NOT_FOUND = "HTTP/1.1 404 Not Found\r\n\r\n"

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

type PaymentRequest struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

type CombinedPaymentsSummary struct {
	Default  PaymentsSummary `json:"default"`
	Fallback PaymentsSummary `json:"fallback"`
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

type TrackRequest struct {
	Processor string  `json:"processor"`
	Amount    float64 `json:"amount"`
	Time      string  `json:"time"`
}

func parseJson(buffer []byte) (string, float64, error) {
	amount := AMOUNT
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
			if amount != 0 {
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
		message := fmt.Sprintf("%s;%f\n", correlationId, amount)
		_, err := udpClient.QueueConn.Write([]byte(message))
		if err != nil {
			log.Printf("sending message to payment queue: %v\n", err)
			return
		}
	}()

	ctx.SetStatusCode(200)
}

func paymentsSummary(ctx *fasthttp.RequestCtx) {
	from := ctx.URI().QueryArgs().PeekBytes([]byte("from"))
	to := ctx.URI().QueryArgs().PeekBytes([]byte("to"))

	message := fmt.Sprintf("s%s;%s\n", string(from), string(to))

	_, err := udpClient.TrackerConn.Write([]byte(message))
	if err != nil {
		log.Printf("sending message to tracker: %v\n", err)
		ctx.SetStatusCode(500)
		return
	}

	trackerResponse, err := bufio.NewReader(udpClient.TrackerConn).ReadString('\n')
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

	ctx.Success("application/json", []byte(j))
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/payments":
		payments(ctx)
	case "/payments-summary":
		paymentsSummary(ctx)
	}
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

	s := &fasthttp.Server{
		Handler:     requestHandler,
		IdleTimeout: 5 * time.Minute,
	}

	log.Printf("listening on port %v\n", serverAddress)
	if err = s.ListenAndServe(serverAddress); err != nil {
		log.Printf("fasthttp listen and serve error: %v\n", err)
	}
}
