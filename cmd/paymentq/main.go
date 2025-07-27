package main

import (
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
	"sync"
	"time"
)

var DEFAULT_PROCESSOR_URL = os.Getenv("DEFAULT_PROCESSOR_URL")
var DEFAULT_PAYMENTS_ENDPOINT = DEFAULT_PROCESSOR_URL + "/payments"
var DEFAULT_SERVICE_HEALTH_ENDPOINT = DEFAULT_PROCESSOR_URL + "/payments/service-health"

var FALLBACK_PROCESSOR_URL = os.Getenv("FALLBACK_PROCESSOR_URL")
var FALLBACK_PAYMENTS_ENDPOINT = FALLBACK_PROCESSOR_URL + "/payments"
var FALLBACK_SERVICE_HEALTH_ENDPOINT = FALLBACK_PROCESSOR_URL + "/payments/service-health"

const MAX_TIMEOUT_IN_MS = 200

var paymentsQueue chan PaymentRequest = make(chan PaymentRequest, 10_000)
var retriesQueue chan PaymentRequest = make(chan PaymentRequest, 1000)

var udpClient struct {
	ListenerConn *net.UDPConn
	TrackerConn  *net.UDPConn
}

var httpClient http.Client = http.Client{
	Timeout: 200 * time.Millisecond,
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
		return errors.New(response.Status)
	}

	return nil
}

func trackPayment(pr PaymentRequest, processor string) {
	message := fmt.Sprintf("t%s;%f;%s\n", processor, pr.Amount, pr.RequestedAt)

	_, err := udpClient.TrackerConn.Write([]byte(message))
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

	return nil
}

func ListenUDP(addressString string) (*net.UDPConn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addressString)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func DialUDP(addressString string, conn **net.UDPConn) error {
	udpAddr, err := net.ResolveUDPAddr("udp", addressString)
	if err != nil {
		return err
	}

	*conn, err = net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	// queues
	for i := 0; i < 1000; i++ {
		go func() {
			pr := <-paymentsQueue
			err := tryProcessing(pr)

			if err != nil {
				retriesQueue <- pr
				log.Printf("retrying")
			}
		}()
	}

	for i := 0; i < 1000; i++ {
		go func() {
			pr := <-retriesQueue
			err := tryProcessing(pr)

			if err != nil {
				retriesQueue <- pr
			}
		}()
	}

	// networking
	var err error
	udpClient.ListenerConn, err = ListenUDP(os.Getenv("ADDRESS"))
	if err != nil {
		log.Printf("initing udp connection: %v\n", err)
		return
	}
	defer udpClient.ListenerConn.Close()

	err = DialUDP(os.Getenv("TRACKER_URL"), &udpClient.TrackerConn)
	if err != nil {
		log.Printf("initing udp connection: %v\n", err)
		return
	}
	defer udpClient.TrackerConn.Close()

	for {
		var buf [512]byte

		n, _, err := udpClient.ListenerConn.ReadFromUDP(buf[:])
		if err != nil {
			log.Printf("reading from connection: %v\n", err)
			continue
		}

		go func() {
			data := buf[:n-2]

			params := strings.Split(string(data[:]), ";")
			amount, err := strconv.ParseFloat(params[1], 64)
			if err != nil {
				log.Printf("%v\n", err)
				log.Printf("amount: %v", params[1])
				return
			}

			paymentsQueue <- PaymentRequest{amount, params[0], ""}
		}()
	}

}
