package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

type Address struct {
	StreetAddress string `json:"STREET_ADDRESS"`
	City          string `json:"CITY"`
	State         string `json:"STATE"`
	PostalCode    string `json:"POSTALCODE"`
}

type EmergencyContact struct {
	Name  string `json:"NAME"`
	Phone string `json:"PHONE"`
}

type LiftTicket struct {
	TxID             string            `json:"TXID"`
	RFID             string            `json:"RFID"`
	Resort           string            `json:"RESORT"`
	PurchaseTime     string            `json:"PURCHASE_TIME"`
	ExpirationTime   string            `json:"EXPIRATION_TIME"`
	Days             int               `json:"DAYS"`
	Name             string            `json:"NAME"`
	Address          *Address          `json:"ADDRESS"`
	Phone            *string           `json:"PHONE"`
	Email            *string           `json:"EMAIL"`
	EmergencyContact *EmergencyContact `json:"EMERGENCY_CONTACT"`
}

// rows POST body is line-delimited JSON objects; continuationToken is passed via querystring

type openChannelResponse struct {
	NextContinuationToken string `json:"next_continuation_token"`
}

func main() {
	var (
		account      string
		jwtToken     string
		databaseName string
		schemaName   string
		pipeName     string
		batchSize    int
		count        int
		bufferSize   int
		eps          float64
	)

	flag.StringVar(&account, "account", envOr("ACCOUNT", ""), "Snowflake account name (or ACCOUNT env)")
	flag.StringVar(&jwtToken, "jwt", envOr("JWT", ""), "Snowflake KEYPAIR JWT (or JWT env)")
	flag.StringVar(&databaseName, "database", envOr("DATABASE", ""), "Snowflake database name (or DATABASE env)")
	flag.StringVar(&schemaName, "schema", envOr("SCHEMA", ""), "Snowflake schema name (or SCHEMA env)")
	flag.StringVar(&pipeName, "pipe", envOr("PIPE", ""), "Snowflake pipe name (or PIPE env)")
	flag.IntVar(&batchSize, "batch", envOrInt("BATCH", 100), "Batch size for rows requests")
	flag.IntVar(&count, "count", envOrInt("COUNT", 100), "Number of events to generate; 0 or negative for infinite")
	flag.IntVar(&bufferSize, "buffer", envOrInt("BUFFER", 100), "Buffered channel size")
	flag.Float64Var(&eps, "eps", envOrFloat("EPS", 140), "Events per second")
	flag.Parse()

	if account == "" || jwtToken == "" || databaseName == "" || schemaName == "" || pipeName == "" {
		log.Fatal("missing required flags: -account, -jwt, -database, -schema, -pipe (or corresponding env vars)")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	events := make(chan LiftTicket, bufferSize)
	var wg sync.WaitGroup

	client := &http.Client{Timeout: 10 * time.Second}
	rowsClient := &http.Client{Timeout: 60 * time.Second}

	// Resolve ingest host via Snowflake control plane
	controlHost := account + ".snowflakecomputing.com"
	controlURL := "https://" + controlHost + "/v2/streaming/hostname"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, controlURL, nil)
	if err != nil {
		log.Fatalf("build control request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+jwtToken)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("control request failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Fatalf("control non-2xx: %s body=%s", resp.Status, string(body))
	}

	// Try to parse ingest host from response
	ingestHost := strings.TrimSpace(string(body))
	// Handle JSON responses if present
	if len(ingestHost) > 0 && (ingestHost[0] == '{' || ingestHost[0] == '"') {
		var parsed map[string]any
		if err := json.Unmarshal(body, &parsed); err == nil {
			for _, k := range []string{"hostname"} {
				if v, ok := parsed[k]; ok {
					if s, ok := v.(string); ok && s != "" {
						ingestHost = s
					}
				}
			}
		}
		ingestHost = strings.Trim(ingestHost, "\"")
	}
	if ingestHost == "" {
		log.Fatal("could not resolve ingest host from control response")
	} else {
		log.Printf("ingest host: %s", ingestHost)
	}

	// Create channel name and final ingest endpoint
	chanName := strings.ReplaceAll(uuid.NewString(), "-", "")
	channelURL := fmt.Sprintf(
		"https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s/channels/%s",
		ingestHost,
		url.PathEscape(databaseName),
		url.PathEscape(schemaName),
		url.PathEscape(pipeName),
		url.PathEscape(chanName),
	)
	log.Printf("control_host=%s ingest_host=%s channel=%s", controlHost, ingestHost, chanName)

	// Open channel (PUT) and get initial continuation token
	openReq, err := http.NewRequestWithContext(ctx, http.MethodPut, channelURL, bytes.NewReader([]byte("{}")))
	if err != nil {
		log.Fatalf("build open channel request: %v", err)
	}
	openReq.Header.Set("Authorization", "Bearer "+jwtToken)
	openReq.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
	openReq.Header.Set("Content-Type", "application/json")
	openReq.Header.Set("Accept", "application/json")
	openResp, err := client.Do(openReq)
	if err != nil {
		log.Fatalf("open channel failed: %v", err)
	}
	openBody, _ := io.ReadAll(openResp.Body)
	_ = openResp.Body.Close()
	if openResp.StatusCode < 200 || openResp.StatusCode >= 300 {
		log.Fatalf("open channel non-2xx: %s body=%s", openResp.Status, string(openBody))
	}
	var oc openChannelResponse
	if err := json.Unmarshal(openBody, &oc); err != nil {
		log.Fatalf("parse open channel response: %v body=%s", err, string(openBody))
	}
	if oc.NextContinuationToken == "" {
		log.Fatalf("missing next_continuation_token in open channel response: %s", string(openBody))
	}
	var tokenMu sync.RWMutex
	continuationToken := oc.NextContinuationToken
	log.Printf("channel opened, continuationToken=%s", continuationToken)

	rowsURL := fmt.Sprintf(
		"https://%s/v2/streaming/data/databases/%s/schemas/%s/pipes/%s/channels/%s/rows",
		ingestHost,
		url.PathEscape(databaseName),
		url.PathEscape(schemaName),
		url.PathEscape(pipeName),
		url.PathEscape(chanName),
	)

	// Ensure channel is closed on exit or error
	defer func() {
		// Use a fresh context independent of main ctx to allow cleanup after cancel
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		dreq, err := http.NewRequestWithContext(dctx, http.MethodDelete, channelURL, nil)
		if err != nil {
			log.Printf("build channel delete request error: %v", err)
			return
		}
		dreq.Header.Set("Authorization", "Bearer "+jwtToken)
		dreq.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
		dreq.Header.Set("Accept", "application/json")
		dresp, err := client.Do(dreq)
		if err != nil {
			log.Printf("channel delete error: %v", err)
			return
		}
		b, _ := io.ReadAll(dresp.Body)
		_ = dresp.Body.Close()
		if dresp.StatusCode < 200 || dresp.StatusCode >= 300 {
			log.Printf("channel delete non-2xx: %s body=%s", dresp.Status, string(b))
			return
		}
		log.Printf("channel deleted successfully")
	}()

	// Consumer goroutine: send events to rows endpoint in batches
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch := make([]json.RawMessage, 0, batchSize)
		consumerStart := time.Now()
		var totalBytesSent int64
		flush := func() {
			if len(batch) == 0 {
				return
			}
			tokenMu.RLock()
			ct := continuationToken
			tokenMu.RUnlock()
			if ct == "" {
				log.Printf("missing continuation token; skipping batch of %d", len(batch))
				batch = batch[:0]
				return
			}
			var sb strings.Builder
			for i := range batch {
				sb.Write(batch[i])
				sb.WriteByte('\n')
			}
			bodyStr := sb.String()
			rowsEndpoint := rowsURL + "?continuationToken=" + url.QueryEscape(ct)
			// Use independent context with its own timeout to avoid cancellation during shutdown
			rowsCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(rowsCtx, http.MethodPost, rowsEndpoint, bytes.NewReader([]byte(bodyStr)))
			if err != nil {
				log.Printf("request build error: %v", err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+jwtToken)
			req.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
			req.Header.Set("Accept", "application/json")
			resp, err := rowsClient.Do(req)
			if err != nil {
				log.Printf("rows POST error: %v", err)
				return
			}
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Printf("rows non-2xx response: %s body=%s", resp.Status, string(respBody))
				return
			}
			var rr struct {
				NextContinuationToken string `json:"next_continuation_token"`
			}
			if err := json.Unmarshal(respBody, &rr); err == nil && rr.NextContinuationToken != "" {
				tokenMu.Lock()
				continuationToken = rr.NextContinuationToken
				tokenMu.Unlock()
			}
			totalBytesSent += int64(len(bodyStr))
			elapsedSeconds := time.Since(consumerStart).Seconds()
			totalMB := float64(totalBytesSent) / (1024.0 * 1024.0)
			mbPerSec := 0.0
			if elapsedSeconds > 0 {
				mbPerSec = totalMB / elapsedSeconds
			}
			log.Printf("sent events=%d totalMB=%.3f seconds=%.1f MB/sec=%.3f", len(batch), totalMB, elapsedSeconds, mbPerSec)
			batch = batch[:0]
		}
		for event := range events {
			payload, err := json.Marshal(event)
			if err != nil {
				log.Printf("marshal error: %v", err)
				continue
			}
			batch = append(batch, json.RawMessage(payload))
			if len(batch) >= batchSize {
				flush()
			}
		}
		// flush remaining on close
		flush()
	}()

	// Producer goroutine: generate events and send into channel
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(events)
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		// Determine interval from eps
		if eps <= 0 {
			eps = 140
		}
		interval := time.Duration(float64(time.Second) / eps)
		if interval <= 0 {
			interval = time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		produce := func() bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}
			event := generateLiftTicket(rng)
			select {
			case events <- event:
				return true
			case <-ctx.Done():
				return false
			}
		}

		if count <= 0 {
			for {
				select {
				case <-ticker.C:
					if !produce() {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		} else {
			for i := 0; i < count; i++ {
				select {
				case <-ticker.C:
					if !produce() {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Wait for completion
	if count > 0 {
		wg.Wait()
		log.Println("completed finite run")
		return
	}

	// Infinite mode: wait for signal, then wait for goroutines to exit
	<-ctx.Done()
	wg.Wait()
	log.Println("shutdown complete")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var out int
		_, err := fmt.Sscanf(v, "%d", &out)
		if err == nil {
			return out
		}
	}
	return def
}

func envOrFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		var out float64
		_, err := fmt.Sscanf(v, "%f", &out)
		if err == nil {
			return out
		}
	}
	return def
}
