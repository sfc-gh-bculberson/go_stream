package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/google/uuid"
)

func MaxParallelism() int {
	maxProcs := runtime.GOMAXPROCS(0)
	numCPU := runtime.NumCPU()
	if maxProcs < numCPU {
		return maxProcs
	}
	return numCPU
}

// loggingRoundTripper logs HTTP method, URL, status, and duration for each request.
type loggingRoundTripper struct {
	base http.RoundTripper
}

func (l loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	resp, err := l.base.RoundTrip(req)
	dur := time.Since(start)
	urlStr := req.URL.String()
	if len(urlStr) > 75 {
		urlStr = urlStr[:75] + "..."
	}
	if err != nil {
		log.Printf("http %s %s error=%v dur=%s", req.Method, urlStr, err, dur)
		return nil, err
	}
	log.Printf("http %s %s status=%d dur=%s", req.Method, urlStr, resp.StatusCode, dur)
	return resp, nil
}

func main() {
	var (
		account      string
		patToken     string
		databaseName string
		schemaName   string
		pipeName     string
		batchSize    int
		count        int
		bufferSize   int
		eps          float64
		useGzip      bool
	)

	flag.StringVar(&account, "account", envOr("ACCOUNT", ""), "Snowflake account name (or ACCOUNT env)")
	flag.StringVar(&patToken, "pat", envOr("PAT", ""), "Snowflake PAT (or PAT env)")
	flag.StringVar(&databaseName, "database", envOr("DATABASE", ""), "Snowflake database name (or DATABASE env)")
	flag.StringVar(&schemaName, "schema", envOr("SCHEMA", ""), "Snowflake schema name (or SCHEMA env)")
	flag.StringVar(&pipeName, "pipe", envOr("PIPE", ""), "Snowflake pipe name (or PIPE env)")
	flag.IntVar(&batchSize, "batch", envOrInt("BATCH", 1000), "Batch size for rows requests")
	flag.IntVar(&count, "count", envOrInt("COUNT", 10000), "Number of events to generate; 0 or negative for infinite")
	flag.IntVar(&bufferSize, "buffer", envOrInt("BUFFER", 1000), "Buffered channel size")
	flag.Float64Var(&eps, "eps", envOrFloat("EPS", 1000), "Events per second")
	flag.BoolVar(&useGzip, "gzip", envOrBool("GZIP", false), "Gzip-compress rows request body")
	flag.Parse()

	// Print effective configuration after flags/env processing (mask PAT)
	maskedPAT := patToken
	if maskedPAT != "" {
		if len(maskedPAT) > 8 {
			maskedPAT = maskedPAT[:4] + "..." + maskedPAT[len(maskedPAT)-4:]
		} else {
			maskedPAT = "****"
		}
	}
	log.Printf("config account=%s database=%s schema=%s pipe=%s batch=%d count=%d buffer=%d eps=%.2f pat=%s, gzip=%t, maxprocs=%d",
		account, databaseName, schemaName, pipeName, batchSize, count, bufferSize, eps, maskedPAT, useGzip, MaxParallelism())

	if account == "" || patToken == "" || databaseName == "" || schemaName == "" || pipeName == "" {
		log.Fatal("missing required flags: -account, -pat, -database, -schema, -pipe (or corresponding env vars)")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	events := make(chan LiftTicket, bufferSize)
	var wg sync.WaitGroup

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   512,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	loggingTransport := &loggingRoundTripper{base: transport}
	client := &http.Client{Transport: loggingTransport, Timeout: 30 * time.Second}
	rowsClient := &http.Client{Transport: loggingTransport, Timeout: 60 * time.Second}

	// Resolve ingest host via helper with retry
	rngStart := rand.New(rand.NewSource(time.Now().UnixNano()))
	maxBackoff := 5 * time.Minute
	backoff := 1 * time.Second
	var ingestHost string
	for {
		var err error
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, 30*time.Second)
		ingestHost, err = resolveIngestHost(attemptCtx, client, account, patToken)
		cancelAttempt()
		if err == nil {
			break
		}
		log.Printf("resolve ingest host failed: %v -- retrying", err)
		sleep := time.Duration((0.5 + rngStart.Float64()) * float64(backoff))
		if sleep > maxBackoff {
			sleep = maxBackoff
		}
		time.Sleep(sleep)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	log.Printf("ingest host: %s", ingestHost)

	// Create channel name and final ingest endpoint
	chanName := strings.ReplaceAll(uuid.NewString(), "-", "")
	// Open channel via helper with retry
	backoff = 1 * time.Second
	var channelURL string
	var rowsURL string
	var continuationToken string
	for {
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, 30*time.Second)
		oc2, cu, ru, err := openChannel(attemptCtx, client, patToken, ingestHost, databaseName, schemaName, pipeName, chanName)
		cancelAttempt()
		if err == nil {
			channelURL = cu
			rowsURL = ru
			continuationToken = oc2.NextContinuationToken
			break
		}
		log.Printf("open channel failed: %v -- retrying", err)
		sleep := time.Duration((0.5 + rngStart.Float64()) * float64(backoff))
		if sleep > maxBackoff {
			sleep = maxBackoff
		}
		time.Sleep(sleep)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	log.Printf("channel opened, continuationToken=%s", continuationToken)

	// Monitor goroutine: poll bulk-channel-status every 1m
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s, code, data, err := bulkChannelStatus(ctx, client, patToken, ingestHost, databaseName, schemaName, pipeName, []string{chanName})
				if err != nil {
					log.Printf("status error: code=%d err=%v body=%s", code, err, string(data))
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
					if cs.ChannelStatusCode != "SUCCESS" {
						log.Printf("status channel=%s is in err state -- reopening channel", chanName)
						reopenBackoff := 1 * time.Second
						rngReopen := rand.New(rand.NewSource(time.Now().UnixNano()))
						for {
							if ctx.Err() != nil {
								log.Printf("reopen aborted: context cancelled")
								break
							}
							attemptCtx, cancelAttempt := context.WithTimeout(ctx, 30*time.Second)
							oc2, _, _, e := openChannel(attemptCtx, client, patToken, ingestHost, databaseName, schemaName, pipeName, chanName)
							cancelAttempt()
							if e == nil {
								continuationToken = oc2.NextContinuationToken
								log.Printf("reopened channel=%s", chanName)
								break
							}
							log.Printf("reopen channel failed: %v -- retrying", e)
							sleep := time.Duration((0.5 + rngReopen.Float64()) * float64(reopenBackoff))
							if sleep > maxBackoff {
								sleep = maxBackoff
							}
							time.Sleep(sleep)
							reopenBackoff *= 2
							if reopenBackoff > maxBackoff {
								reopenBackoff = maxBackoff
							}
						}
					}
				} else {
					log.Printf("status channel=%s not found in response", chanName)
				}
			}
		}
	}()

	// Ensure channel is closed on exit only
	defer func() {
		if err := dropChannel(context.Background(), client, patToken, channelURL); err != nil {
			log.Printf("channel delete error: %v", err)
		} else {
			log.Printf("channel delete attempted on exit")
		}
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
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		flush := func() {
			if len(batch) == 0 {
				return
			}
			var sb strings.Builder
			for i := range batch {
				sb.Write(batch[i])
				sb.WriteByte('\n')
			}
			bodyStr := sb.String()
			nowMs := time.Now().UnixMilli()
			rowsCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			nextCT, status, _, compressedLen, err := appendRows(rowsCtx, rowsClient, patToken, rowsURL, continuationToken, nowMs, uuid.NewString(), []byte(bodyStr), useGzip)
			cancel()
			if err != nil {
				if status == 400 {
					log.Printf("rows POST error: status=%d err=%v -- reopening channel", status, err)
					rowsReopenCtx, cancelReopen := context.WithTimeout(ctx, 30*time.Second)
					oc2, _, _, e := openChannel(rowsReopenCtx, client, patToken, ingestHost, databaseName, schemaName, pipeName, chanName)
					cancelReopen()
					if e != nil {
						log.Printf("reopen channel failed: %v", e)
						return
					}
					continuationToken = oc2.NextContinuationToken
					log.Printf("reopened channel=%s", chanName)
					status = 0
				}
				log.Printf("rows POST error: status=%d err=%v -- retrying", status, err)
				rowsBackoff := 1
				for {
					rowsCtxAttempt, cancelAttempt := context.WithTimeout(context.Background(), 60*time.Second)
					nextCT, status, _, compressedLen, err = appendRows(rowsCtxAttempt, rowsClient, patToken, rowsURL, continuationToken, nowMs, uuid.NewString(), []byte(bodyStr), useGzip)
					cancelAttempt()
					if err == nil {
						break
					}
					log.Printf("rows POST error: status=%d err=%v -- retrying", status, err)
					base := time.Duration(rowsBackoff) * time.Second
					jitter := 0.5 + rng.Float64() // 50%-150% jitter
					sleep := time.Duration(jitter * float64(base))
					if sleep > 5*time.Minute {
						sleep = 5 * time.Minute
					}
					time.Sleep(sleep)
					rowsBackoff *= 2
					if rowsBackoff > 300 { // cap at 5 minutes
						rowsBackoff = 300
					}
				}
			}
			continuationToken = nextCT
			totalBytesSent += int64(compressedLen)
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
			event := generateLiftTicket()
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

func envOrBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	case "0", "false", "f", "no", "n", "off":
		return false
	default:
		return def
	}
}
