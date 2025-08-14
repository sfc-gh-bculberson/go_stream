package main

import (
	"bytes"
	"compress/gzip"
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
	"strconv"
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

type bulkStatusRequest struct {
	ChannelNames []string `json:"channel_names"`
}

type channelStatus struct {
	ChannelStatusCode               string `json:"channel_status_code"`
	LastCommittedOffsetToken        string `json:"last_committed_offset_token"`
	DatabaseName                    string `json:"database_name"`
	SchemaName                      string `json:"schema_name"`
	PipeName                        string `json:"pipe_name"`
	ChannelName                     string `json:"channel_name"`
	RowsInserted                    int64  `json:"rows_inserted"`
	RowsParsed                      int64  `json:"rows_parsed"`
	RowsErrors                      int64  `json:"rows_errors"`
	LastErrorOffsetUpperBound       string `json:"last_error_offset_upper_bound"`
	LastErrorMessage                string `json:"last_error_message"`
	LastErrorTimestamp              string `json:"last_error_timestamp"`
	SnowflakeAvgProcessingLatencyMs int64  `json:"snowflake_avg_processing_latency_ms"`
}

type bulkStatusResponse struct {
	ChannelStatuses map[string]channelStatus `json:"channel_statuses"`
}

type openChannelResponse struct {
	NextContinuationToken string        `json:"next_continuation_token"`
	ChannelStatus         channelStatus `json:"channel_status"`
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
	flag.IntVar(&batchSize, "batch", envOrInt("BATCH", 1000), "Batch size for rows requests")
	flag.IntVar(&count, "count", envOrInt("COUNT", 10000), "Number of events to generate; 0 or negative for infinite")
	flag.IntVar(&bufferSize, "buffer", envOrInt("BUFFER", 1000), "Buffered channel size")
	flag.Float64Var(&eps, "eps", envOrFloat("EPS", 1000), "Events per second")
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
	// Append requestId per spec
	uOpen, _ := url.Parse(channelURL)
	qOpen := uOpen.Query()
	qOpen.Set("requestId", uuid.NewString())
	uOpen.RawQuery = qOpen.Encode()
	openReq, err := http.NewRequestWithContext(ctx, http.MethodPut, uOpen.String(), bytes.NewReader([]byte("{}")))
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

	// Helpers to (re)open and close channels
	openChannel := func() (newChanName, newChannelURL, newRowsURL, newToken string, err error) {
		name := strings.ReplaceAll(uuid.NewString(), "-", "")
		chURL := fmt.Sprintf(
			"https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s/channels/%s",
			ingestHost,
			url.PathEscape(databaseName),
			url.PathEscape(schemaName),
			url.PathEscape(pipeName),
			url.PathEscape(name),
		)
		u, _ := url.Parse(chURL)
		q := u.Query()
		q.Set("requestId", uuid.NewString())
		u.RawQuery = q.Encode()
		req, e := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), bytes.NewReader([]byte("{}")))
		if e != nil {
			return "", "", "", "", e
		}
		req.Header.Set("Authorization", "Bearer "+jwtToken)
		req.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		resp, e := client.Do(req)
		if e != nil {
			return "", "", "", "", e
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "", "", "", "", fmt.Errorf("open channel non-2xx: %s body=%s", resp.Status, string(body))
		}
		var oc2 openChannelResponse
		if e := json.Unmarshal(body, &oc2); e != nil {
			return "", "", "", "", e
		}
		if oc2.NextContinuationToken == "" {
			return "", "", "", "", fmt.Errorf("missing continuation token")
		}
		rows := fmt.Sprintf(
			"https://%s/v2/streaming/data/databases/%s/schemas/%s/pipes/%s/channels/%s/rows",
			ingestHost,
			url.PathEscape(databaseName),
			url.PathEscape(schemaName),
			url.PathEscape(pipeName),
			url.PathEscape(name),
		)
		return name, chURL, rows, oc2.NextContinuationToken, nil
	}

	// Drop Channel per spec with optional requestId
	closeChannel := func(chURL string) {
		if chURL == "" {
			return
		}
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Append requestId query parameter
		u, err := url.Parse(chURL)
		if err != nil {
			return
		}
		q := u.Query()
		q.Set("requestId", uuid.NewString())
		u.RawQuery = q.Encode()
		dreq, err := http.NewRequestWithContext(dctx, http.MethodDelete, u.String(), nil)
		if err != nil {
			return
		}
		dreq.Header.Set("Authorization", "Bearer "+jwtToken)
		dreq.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
		dreq.Header.Set("Accept", "application/json")
		dresp, err := client.Do(dreq)
		if err == nil {
			_ = dresp.Body.Close()
		}
	}

	// Monitor goroutine: poll bulk-channel-status every 5s
	monitorURL := fmt.Sprintf(
		"https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s:bulk-channel-status",
		ingestHost,
		url.PathEscape(databaseName),
		url.PathEscape(schemaName),
		url.PathEscape(pipeName),
	)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				reqBody := bulkStatusRequest{ChannelNames: []string{chanName}}
				b, _ := json.Marshal(reqBody)
				r, err := http.NewRequestWithContext(ctx, http.MethodPost, monitorURL, bytes.NewReader(b))
				if err != nil {
					log.Printf("status request build error: %v", err)
					continue
				}
				r.Header.Set("Content-Type", "application/json")
				r.Header.Set("Authorization", "Bearer "+jwtToken)
				r.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
				r.Header.Set("Accept", "application/json")
				resp, err := client.Do(r)
				if err != nil {
					log.Printf("status request error: %v", err)
					continue
				}
				data, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode < 200 || resp.StatusCode >= 300 {
					log.Printf("status non-2xx: %s body=%s", resp.Status, string(data))
					continue
				}
				var s bulkStatusResponse
				if err := json.Unmarshal(data, &s); err != nil {
					log.Printf("status parse error: %v body=%s", err, string(data))
					continue
				}
				if cs, ok := s.ChannelStatuses[chanName]; ok {
					lagMs := int64(-1)
					if cs.LastCommittedOffsetToken != "" {
						if tokenMs, err := strconv.ParseInt(cs.LastCommittedOffsetToken, 10, 64); err == nil {
							lagMs = time.Now().UnixMilli() - tokenMs
						} else {
							log.Printf("status channel=%s last_commit_parse_error=%v raw=%q", chanName, err, cs.LastCommittedOffsetToken)
						}
					}
					log.Printf("status channel=%s code=%s rows_inserted=%d rows_parsed=%d rows_errors=%d last_commit=%s latency_ms=%d lag_ms=%d", cs.ChannelName, cs.ChannelStatusCode, cs.RowsInserted, cs.RowsParsed, cs.RowsErrors, cs.LastCommittedOffsetToken, cs.SnowflakeAvgProcessingLatencyMs, lagMs)
				} else {
					log.Printf("status channel=%s not found in response", chanName)
				}
			}
		}
	}()

	// Ensure channel is closed on exit only
	defer func() {
		closeChannel(channelURL)
		log.Printf("channel delete attempted on exit")
	}()

	// Consumer goroutine: send events to rows endpoint in batches
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch := make([]json.RawMessage, 0, batchSize)
		consumerStart := time.Now()
		lastReport := consumerStart
		var totalBytesSent int64             // compressed bytes
		var totalUncompressedBytesSent int64 // raw (uncompressed) bytes
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
			nowMs := time.Now().UnixMilli()
			rowsEndpoint := rowsURL + "?continuationToken=" + url.QueryEscape(ct) + "&offsetToken=" + strconv.FormatInt(nowMs, 10) + "&requestId=" + url.QueryEscape(uuid.NewString())
			// Use independent context with its own timeout to avoid cancellation during shutdown
			rowsCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			// gzip-compress the body
			var gzBuf bytes.Buffer
			gzWriter := gzip.NewWriter(&gzBuf)
			_, _ = gzWriter.Write([]byte(bodyStr))
			_ = gzWriter.Close()
			req, err := http.NewRequestWithContext(rowsCtx, http.MethodPost, rowsEndpoint, bytes.NewReader(gzBuf.Bytes()))
			if err != nil {
				log.Printf("request build error: %v", err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+jwtToken)
			req.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
			req.Header.Set("Accept", "application/json")
			req.Header.Set("Content-Encoding", "gzip")
			resp, err := rowsClient.Do(req)
			if err != nil {
				log.Printf("rows POST error: %v -- reopening channel", err)
				var e error
				chanName, channelURL, rowsURL, continuationToken, e = openChannel()
				if e != nil {
					log.Printf("reopen channel failed: %v", e)
					return
				}
				log.Printf("reopened channel=%s", chanName)
				return
			}
			respBody, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				log.Printf("rows non-2xx response: %s body=%s -- reopening channel", resp.Status, string(respBody))
				var e error
				chanName, channelURL, rowsURL, continuationToken, e = openChannel()
				if e != nil {
					log.Printf("reopen channel failed: %v", e)
					return
				}
				log.Printf("reopened channel=%s", chanName)
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
			totalBytesSent += int64(gzBuf.Len())
			totalUncompressedBytesSent += int64(len(bodyStr))
			elapsedSeconds := time.Since(consumerStart).Seconds()
			totalMBCompressed := float64(totalBytesSent) / (1024.0 * 1024.0)
			totalMBUncompressed := float64(totalUncompressedBytesSent) / (1024.0 * 1024.0)
			mbPerSecCompressed := 0.0
			mbPerSecUncompressed := 0.0
			if elapsedSeconds > 0 {
				mbPerSecCompressed = totalMBCompressed / elapsedSeconds
				mbPerSecUncompressed = totalMBUncompressed / elapsedSeconds
			}
			if time.Since(lastReport) >= 5*time.Second {
				log.Printf("throughput totalMB_uncompressed=%.3f totalMB_compressed=%.3f seconds=%.1f MB/sec_uncompressed=%.3f MB/sec_compressed=%.3f", totalMBUncompressed, totalMBCompressed, elapsedSeconds, mbPerSecUncompressed, mbPerSecCompressed)
				lastReport = time.Now()
			}
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
