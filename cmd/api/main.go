package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unicode"

	"github.com/tv42/httpunix"
	"github.com/valyala/fasthttp"
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

const HEALTH_CHECKER_INTERVAL = 6 * time.Second
const MAX_TIMEOUT_IN_MS = 150

var OTHER_BACKEND_URI = ""
var OTHER_BACKEND_ENDPOINT = ""

var paymentsQueue chan *PaymentRequest
var trackerQueue chan TrackRequest

var serverConn *net.UnixConn

var httpClient http.Client = http.Client{
	Timeout: MAX_TIMEOUT_IN_MS * time.Millisecond,
}

var unixClient http.Client

var bytesBufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

type PaymentsSummary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

type CombinedPaymentsSummary struct {
	Default  PaymentsSummary `json:"default"`
	Fallback PaymentsSummary `json:"fallback"`
}

type AmountWithTime struct {
	Amount float64
	Time   time.Time
}

type StatsTracker struct {
	Default                 PaymentsSummary `json:"default"`
	Fallback                PaymentsSummary `json:"fallback"`
	DefaultAmountsWithTime  []AmountWithTime
	FallbackAmountsWithTime []AmountWithTime
	mu                      sync.Mutex
}

type TrackRequest struct {
	Processor string  `json:"processor"`
	Amount    float64 `json:"amount"`
	Time      string  `json:"time"`
}

type Tracker struct {
	Default                 PaymentsSummary `json:"default"`
	Fallback                PaymentsSummary `json:"fallback"`
	DefaultAmountsWithTime  []AmountWithTime
	FallbackAmountsWithTime []AmountWithTime
	mu                      sync.Mutex
}

func (t *Tracker) Track(tr TrackRequest) {
	t.mu.Lock()
	defer t.mu.Unlock()

	timestamp, err := time.Parse(time.RFC3339Nano, tr.Time)
	if err != nil {
		log.Printf("parsing timestamp: %v\n", err)
		return
	}

	if tr.Processor == "default" {
		t.Default.TotalRequests++
		t.Default.TotalAmount += tr.Amount
		t.DefaultAmountsWithTime = append(t.DefaultAmountsWithTime, AmountWithTime{tr.Amount, timestamp})
	} else {
		t.Fallback.TotalRequests++
		t.Fallback.TotalAmount += tr.Amount
		t.FallbackAmountsWithTime = append(t.FallbackAmountsWithTime, AmountWithTime{tr.Amount, timestamp})
	}
}

var tracker Tracker

func (t *Tracker) RangedSummary(from, to *time.Time) CombinedPaymentsSummary {
	t.mu.Lock()
	defer t.mu.Unlock()

	var defaultAmount, fallbackAmount float64
	var defaultRequests, fallbackRequests int

	for _, p := range t.DefaultAmountsWithTime {
		inRange := true

		if from != nil && p.Time.Before(*from) {
			inRange = false
		}

		if to != nil && p.Time.After(*to) {
			inRange = false
		}

		if inRange {
			defaultAmount += p.Amount
			defaultRequests++
		}
	}

	for _, p := range t.FallbackAmountsWithTime {
		inRange := true

		if from != nil && p.Time.Before(*from) {
			inRange = false
		}

		if to != nil && p.Time.After(*to) {
			inRange = false
		}

		if inRange {
			fallbackAmount += p.Amount
			fallbackRequests++
		}
	}

	return CombinedPaymentsSummary{
		Default: PaymentsSummary{
			TotalRequests: defaultRequests,
			TotalAmount:   defaultAmount,
		},
		Fallback: PaymentsSummary{
			TotalRequests: fallbackRequests,
			TotalAmount:   fallbackAmount,
		},
	}
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

	buf := bytesBufferPool.Get().(*bytes.Buffer)
	defer bytesBufferPool.Put(buf)

	buf.Reset()
	buf.Write(payload)

	response, err := httpClient.Post(endpoint, "application/json", buf)
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
		if response.StatusCode == http.StatusUnprocessableEntity {
			return nil
		}

		return errors.New(response.Status)
	}

	return nil
}

func tryProcessing(pr *PaymentRequest) error {
	defaultTime := healthChecker.Default.MinResponseTime
	fallbackTime := healthChecker.Fallback.MinResponseTime

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
	trackerQueue <- TrackRequest{
		Processor: processor,
		Amount:    pr.Amount,
		Time:      pr.RequestedAt,
	}
}

func paymentsSummary(ctx *fasthttp.RequestCtx) {
	args := ctx.QueryArgs()

	argsCopy := fasthttp.Args{}
	args.CopyTo(&argsCopy)

	fromStr := string(argsCopy.Peek("from"))
	toStr := string(argsCopy.Peek("to"))

	var from, to *time.Time
	var err error

	summaryChan := make(chan CombinedPaymentsSummary)

	go func() {
		otherSummary := CombinedPaymentsSummary{}

		fullEndpoint := fmt.Sprintf("%s?from=%s&to=%s", OTHER_BACKEND_ENDPOINT, fromStr, toStr)
		response, err := unixClient.Get(fullEndpoint)
		if err != nil {
			log.Printf("getting summary from other endpoint: %v\n", err)
			summaryChan <- otherSummary
			return
		}

		if response.StatusCode != http.StatusOK {
			log.Printf("getting summary from other endpoint: %v\n", response.Status)
			summaryChan <- otherSummary
			return
		}

		defer response.Body.Close()

		err = json.NewDecoder(response.Body).Decode(&otherSummary)
		if err != nil {
			log.Printf("unmarshalling json: %v\n", err)
		}

		summaryChan <- otherSummary
	}()

	if fromStr != "" {
		parsedFrom, err := time.Parse(time.RFC3339Nano, fromStr)
		if err != nil {
			log.Printf("parsing from timestamp %v\n", err)
			ctx.Error("", 400)
			return
		}

		from = &parsedFrom
	}

	if toStr != "" {
		parsedTo, err := time.Parse(time.RFC3339Nano, toStr)
		if err != nil {
			log.Printf("parsing to timestamp %v\n", err)
			ctx.Error("", 400)
			return
		}

		to = &parsedTo
	}

	thisSummary := tracker.RangedSummary(from, to)

	otherSummary := <-summaryChan
	finalSummary := CombinedPaymentsSummary{}
	finalSummary.Default.TotalAmount = thisSummary.Default.TotalAmount + otherSummary.Default.TotalAmount
	finalSummary.Fallback.TotalAmount = thisSummary.Fallback.TotalAmount + otherSummary.Fallback.TotalAmount

	finalSummary.Default.TotalRequests = thisSummary.Default.TotalRequests + otherSummary.Default.TotalRequests
	finalSummary.Fallback.TotalRequests = thisSummary.Fallback.TotalRequests + otherSummary.Fallback.TotalRequests

	jsonResponse, err := json.Marshal(finalSummary)
	if err != nil {
		log.Printf("marshalling json: %v\n", err)
		ctx.Error("", 500)
		return
	}

	ctx.Success("application/json", jsonResponse)
}

func paymentsSummaryInternal(ctx *fasthttp.RequestCtx) {
	args := ctx.QueryArgs()
	fromStr := string(args.Peek("from"))
	toStr := string(args.Peek("to"))

	var from, to *time.Time
	var err error

	if fromStr != "" {
		parsedFrom, err := time.Parse(time.RFC3339Nano, fromStr)
		if err != nil {
			log.Printf("parsing from timestamp %v\n", err)
			ctx.Error("", 400)
			return
		}

		from = &parsedFrom
	}

	if toStr != "" {
		parsedTo, err := time.Parse(time.RFC3339Nano, toStr)
		if err != nil {
			log.Printf("parsing from timestamp %v\n", err)
			ctx.Error("", 400)
			return
		}

		to = &parsedTo
	}

	thisSummary := tracker.RangedSummary(from, to)

	jsonResponse, err := json.Marshal(thisSummary)
	if err != nil {
		log.Printf("marshalling json: %v\n", err)
		ctx.Error("", 500)
		return
	}

	ctx.Success("application/json", jsonResponse)
}

// zero-allocation json parser
// only parses "correlationId" and "amount" fields
func parseJson(buffer []byte) ([]byte, []byte, error) {
	var correlationId, amount []byte
	var c byte
	var pos int

	advance := func() {
		pos++
		c = buffer[pos]
	}

	for pos = 0; pos < len(buffer); pos++ {
		c = buffer[pos]

		for c == ' ' || c == '\n' || c == '\t' {
			advance()
		}

		if c != '"' {
			continue
		}

		advance()

		if c == 'c' { // correlationId
			for c != ':' {
				advance()
			}

			for c != '"' {
				advance()
			}

			advance() // skip opening "

			stringStart := pos

			for c != '"' {
				advance()
			}

			correlationId = buffer[stringStart:pos]
		} else if c == 'a' { // amount
			for c != ':' {
				advance()
			}

			for !unicode.IsDigit(rune(c)) {
				advance()
			}

			numberStart := pos

			for unicode.IsDigit(rune(c)) {
				advance()
			}

			if c == '.' {
				advance()
			}

			for unicode.IsDigit(rune(c)) {
				advance()
			}

			var err error
			amount = buffer[numberStart:pos]

			if err != nil {
				return correlationId, amount, errors.New("parsing float")
			}
		}
	}

	return correlationId, amount, nil
}

func payments(ctx *fasthttp.RequestCtx) {
	correlationIdBytes, amountBytes, err := parseJson(ctx.PostBody())
	if err != nil {
		log.Printf("parsing json: %v\n", err)
		return
	}

	correlationId := string(correlationIdBytes)

	amount, err := strconv.ParseFloat(string(amountBytes), 64)
	if err != nil {
		log.Printf("parsing float: %v\n", err)
		return
	}

	ctx.Success("text/plain", nil)

	newPr := PaymentRequest{
		Amount:        amount,
		CorrelationID: correlationId,
		RequestedAt:   "",
	}

	paymentsQueue <- &newPr
}

func requestHandler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/payments":
		payments(ctx)
	case "/payments-summary":
		paymentsSummary(ctx)
	case "/payments-summary-internal":
		paymentsSummaryInternal(ctx)
	default:
		ctx.NotFound()
	}
}

func main() {
	serverAddress := os.Getenv("ADDRESS")
	OTHER_BACKEND_URI = os.Getenv("OTHER_BACKEND_URI")
	OTHER_BACKEND_ENDPOINT = fmt.Sprintf("http+unix://otherbackend/payments-summary-internal")

	// boilerplate for communicating between backends
	u := &httpunix.Transport{}
	u.RegisterLocation("otherbackend", OTHER_BACKEND_URI)

	t := &http.Transport{}
	t.RegisterProtocol(httpunix.Scheme, u)
	unixClient = http.Client{
		Transport: t,
	}

	// init queues
	paymentsQueue = make(chan *PaymentRequest, 10_000)
	trackerQueue = make(chan TrackRequest, 10_000)

	try := func(pr *PaymentRequest) {
		err := tryProcessing(pr)

		for err != nil {
			time.Sleep(20 * time.Millisecond)
			err = tryProcessing(pr)
		}
	}

	// consume queues
	for range 4 {
		go func() {
			for pr := range paymentsQueue {
				try(pr)
			}
		}()
	}

	go func() {
		for tr := range trackerQueue {
			tracker.Track(tr)
		}
	}()

	// listen to messages from load balancer
	go func() {
		if err := fasthttp.ListenAndServeUNIX(serverAddress, fs.ModePerm, requestHandler); err != nil {
			log.Fatalf("initing http server")
		}
	}()

	fmt.Println("up and running")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	<-c
	os.Exit(1)
}
