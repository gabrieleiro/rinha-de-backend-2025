package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

const DEFAULT_PAYMENTS_ENDPOINT = "http://127.0.0.1:8001/payments"
const FALLBACK_PAYMENTS_ENDPOINT = "http://127.0.0.1:8002/payments"

const DEFAULT_SERVICE_HEALTH_ENDPOINT = "http://127.0.0.1:8001/payments/service-health"
const FALLBACK_SERVICE_HEALTH_ENDPOINT = "http://127.0.0.1:8002/payments/service-health"

const HEALTH_CHECKER_INTERVAL = 6 * time.Second
const MAX_TIMEOUT_IN_MS = 200

const SERVER_ADDRESS = ":9999"

var client http.Client = http.Client{
	Timeout: 200 * time.Millisecond,
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

type StatsTracker struct {
	Default                 PaymentsSummary `json:"default"`
	Fallback                PaymentsSummary `json:"fallback"`
	DefaultAmountsWithTime  []AmountWithTime
	FallbackAmountsWithTime []AmountWithTime
	mu                      sync.Mutex
}

func (st *StatsTracker) TrackDefault(amount float64, now time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.Default.TotalRequests++
	st.Default.TotalAmount += amount
	st.DefaultAmountsWithTime = append(st.DefaultAmountsWithTime, AmountWithTime{amount, now})
}

func (st *StatsTracker) TrackFallback(amount float64, now time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()

	st.Fallback.TotalRequests++
	st.Fallback.TotalAmount += amount
	st.FallbackAmountsWithTime = append(st.FallbackAmountsWithTime, AmountWithTime{amount, now})
}

func (st *StatsTracker) RangedSummary(from, to *time.Time) CombinedPaymentsSummary {
	st.mu.Lock()
	defer st.mu.Unlock()

	var amountDefault, amountFallback float64
	var requestsDefault, requestsFallback int

	for _, p := range st.DefaultAmountsWithTime {
		inRange := true

		if from != nil && p.Time.Before(*from) {
			inRange = false
		}

		if to != nil && p.Time.After(*to) {
			inRange = false
		}

		if inRange {
			amountDefault += p.Amount
			requestsDefault++
		}
	}

	for _, p := range st.FallbackAmountsWithTime {
		inRange := true

		if from != nil && p.Time.Before(*from) {
			inRange = false
		}

		if to != nil && p.Time.After(*to) {
			inRange = false
		}

		if inRange {
			amountFallback += p.Amount
			requestsFallback++
		}
	}

	return CombinedPaymentsSummary{
		Default: PaymentsSummary{
			TotalAmount:   amountDefault,
			TotalRequests: requestsDefault,
		},
		Fallback: PaymentsSummary{
			TotalAmount:   amountFallback,
			TotalRequests: requestsFallback,
		},
	}
}

var statsTracker = StatsTracker{}

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

func tryProcessing(pr PaymentRequest) error {
	if healthChecker.Default.Failing && healthChecker.Fallback.Failing {
		return errors.New("both processors are down")
	}

	if !healthChecker.Default.Failing && healthChecker.Default.MinResponseTime < MAX_TIMEOUT_IN_MS {
		now := time.Now().UTC()
		err := tryPost(DEFAULT_PAYMENTS_ENDPOINT, &pr, now)

		if err == nil {
			go statsTracker.TrackDefault(pr.Amount, now)
			return nil
		}
	}

	if !healthChecker.Fallback.Failing {
		now := time.Now().UTC()
		err := tryPost(FALLBACK_PAYMENTS_ENDPOINT, &pr, now)

		if err == nil {
			go statsTracker.TrackFallback(pr.Amount, now)
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
		log.Printf("slow: %v\n", elapsed)
	}

	return
}

func paymentsSummary(w http.ResponseWriter, r *http.Request) {
	begin := time.Now()

	fromString := r.URL.Query().Get("from")
	toString := r.URL.Query().Get("to")

	if fromString == "" && toString == "" {
		statsTracker.Default.TotalAmount = float64(int(statsTracker.Default.TotalAmount*10)) / 10
		statsTracker.Fallback.TotalAmount = float64(int(statsTracker.Fallback.TotalAmount*10)) / 10

		ps := CombinedPaymentsSummary{
			Default:  statsTracker.Default,
			Fallback: statsTracker.Fallback,
		}

		json.NewEncoder(w).Encode(ps)

		elapsed := time.Since(begin)

		log.Printf("summary took %v to calculate all\n", elapsed)
		return
	}

	var from, to *time.Time
	if toString != "" {
		parsed, _ := time.Parse(time.RFC3339Nano, toString)
		to = &parsed
	}

	if fromString != "" {
		parsed, _ := time.Parse(time.RFC3339Nano, fromString)
		from = &parsed
	}

	json.NewEncoder(w).Encode(statsTracker.RangedSummary(from, to))

	elapsed := time.Since(begin)
	log.Printf("summary took %v to calculate\n", elapsed)
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

	http.ListenAndServe(SERVER_ADDRESS, nil)
}
