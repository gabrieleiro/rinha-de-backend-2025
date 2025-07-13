package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var client struct {
	Conn *net.UDPConn
}

type PaymentsSummary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
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

type CombinedPaymentsSummary struct {
	Default  PaymentsSummary `json:"default"`
	Fallback PaymentsSummary `json:"fallback"`
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

	var amountDefault, amountFallback float64
	var requestsDefault, requestsFallback int

	for _, p := range t.DefaultAmountsWithTime {
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

	for _, p := range t.FallbackAmountsWithTime {
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

func summary(w http.ResponseWriter, r *http.Request) {
	fromString := r.URL.Query().Get("from")
	toString := r.URL.Query().Get("to")

	if fromString == "" && toString == "" {
		tracker.Default.TotalAmount = float64(int(tracker.Default.TotalAmount*10)) / 10
		tracker.Fallback.TotalAmount = float64(int(tracker.Fallback.TotalAmount*10)) / 10

		ps := CombinedPaymentsSummary{
			Default:  tracker.Default,
			Fallback: tracker.Fallback,
		}

		json.NewEncoder(w).Encode(ps)
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

	json.NewEncoder(w).Encode(tracker.RangedSummary(from, to))
}

func main() {
	address := os.Getenv("ADDRESS")

	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		log.Printf("resolving address: %v\n", err)
		return
	}

	client.Conn, err = net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Printf("dialing connection: %v\n", err)
		return
	}

	for {
		var buf [512]byte

		n, addr, err := client.Conn.ReadFromUDP(buf[0:])
		if err != nil {
			log.Printf("reading from connection: %v\n", err)
			continue
		}

		go func() {
			data := buf[:n-1]

			if string(data[0]) == "t" {
				params := strings.Split(string(data[1:]), ";")
				amount, err := strconv.ParseFloat(params[1], 64)
				if err != nil {
					log.Printf("parsing float: %v\n", err)
					return
				}

				go tracker.Track(TrackRequest{
					Processor: params[0],
					Amount:    amount,
					Time:      params[2],
				})
			} else if string(data[0]) == "s" {
				params := strings.Split(string(data[1:]), ";")
				var from, to *time.Time

				if params[0] != "" {
					t, err := time.Parse(time.RFC3339Nano, params[0])
					if err != nil {
						log.Printf("parsing from timestamp %v\n", err)
						return
					}

					from = &t
				}

				if params[1] != "" {
					t, err := time.Parse(time.RFC3339Nano, params[1])
					if err != nil {
						log.Printf("parsing to timestamp %v\n", err)
						return
					}

					to = &t
				}

				summary := tracker.RangedSummary(from, to)
				response := fmt.Sprintf("%d;%f;%d;%f\n", summary.Default.TotalRequests, summary.Default.TotalAmount, summary.Fallback.TotalRequests, summary.Fallback.TotalAmount)
				client.Conn.WriteToUDP([]byte(response), addr)
			} else {
				log.Printf("unrecognized message: %v\n", string(buf[0:]))
			}
		}()
	}
}
