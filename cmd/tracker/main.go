package main

import (
	"fmt"
	"log"
	"net"
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

func (t *Tracker) RangedSummary(from, to *time.Time) string {
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

	json := fmt.Sprintf(`{ "default": { "totalRequests": %d, "totalAmount": %f }, "fallback": { "totalRequests": %d, "totalAmount": %f } }%s`,
		requestsDefault, amountDefault, requestsFallback, amountFallback, "\n")

	return json
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

	fmt.Println("up and running")

	for {
		var buf [512]byte

		n, addr, err := client.Conn.ReadFromUDP(buf[0:])
		if err != nil {
			log.Printf("reading from connection: %v\n", err)
			continue
		}

		go func() {
			data := buf[:n-1]

			// The first byte of the payload
			// indicates whether it's trying
			// to track a payment that has
			// been processed or get a summary
			// of payments tracked so far
			// 0x0 = track payment
			// 0x1 = get summary

			// The payload format for tracking
			// new payments looks like this:
			// <0x1><default | fallback>;<correlationId>;<amount>\n
			//
			// And for retrieving the summary:
			// 0x1<from>;<to>\n
			if data[0] == 0 {
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
			} else if data[0] == 1 {
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

				if len(params) > 1 && params[1] != "" {
					t, err := time.Parse(time.RFC3339Nano, params[1])
					if err != nil {
						log.Printf("parsing to timestamp %v\n", err)
						return
					}

					to = &t
				}

				summary := tracker.RangedSummary(from, to)
				client.Conn.WriteToUDP([]byte(summary), addr)
			} else {
				log.Printf("unrecognized message: %v\n", string(buf[0:]))
			}
		}()
	}
}
