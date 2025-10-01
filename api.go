package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
)

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

func resolveIngestHost(ctx context.Context, client *http.Client, account, patToken string) (string, error) {
	controlHost := account + ".us-west-2.privatelink.snowflakecomputing.com"
	//controlHost := account + ".snowflakecomputing.com"
	controlURL := "https://" + controlHost + "/v2/streaming/hostname"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, controlURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+patToken)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "PROGRAMMATIC_ACCESS_TOKEN")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("control non-2xx: %s body=%s", resp.Status, string(body))
	}
	ingestHost := string(bytes.TrimSpace(body))
	if len(ingestHost) > 0 && (ingestHost[0] == '{' || ingestHost[0] == '"') {
		var parsed map[string]any
		if err := json.Unmarshal(body, &parsed); err == nil {
			if v, ok := parsed["hostname"]; ok {
				if s, ok := v.(string); ok && s != "" {
					ingestHost = s
				}
			}
		}
		ingestHost = string(bytes.Trim(append([]byte(ingestHost), 0), "\""))
	}
	return ingestHost, nil
}

func openChannel(ctx context.Context, client *http.Client, patToken, ingestHost, database, schema, pipe, channelName string) (openChannelResponse, string, string, error) {
	var oc openChannelResponse
	chURL := fmt.Sprintf("https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s/channels/%s",
		ingestHost, url.PathEscape(database), url.PathEscape(schema), url.PathEscape(pipe), url.PathEscape(channelName))
	u, _ := url.Parse(chURL)
	q := u.Query()
	q.Set("requestId", uuid.NewString())
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), bytes.NewReader([]byte("{}")))
	if err != nil {
		return oc, "", "", err
	}
	req.Header.Set("Authorization", "Bearer "+patToken)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "PROGRAMMATIC_ACCESS_TOKEN")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error with request:", err)
		return oc, "", "", err
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return oc, "", "", fmt.Errorf("open channel non-2xx: %s body=%s", resp.Status, string(body))
	}
	if err := json.Unmarshal(body, &oc); err != nil {
		return oc, "", "", err
	}
	rowsURL := fmt.Sprintf("https://%s/v2/streaming/data/databases/%s/schemas/%s/pipes/%s/channels/%s/rows",
		ingestHost, url.PathEscape(database), url.PathEscape(schema), url.PathEscape(pipe), url.PathEscape(channelName))
	return oc, chURL, rowsURL, nil
}

func appendRows(ctx context.Context, client *http.Client, patToken, rowsURL, continuationToken string, offsetTokenMs int64, requestId string, ndjson []byte, useGzip bool, sessionHeader string) (nextContinuation string, statusCode int, respBody []byte, compressedLen int, sessionHeaderOut string, err error) {
	endpoint := rowsURL + "?continuationToken=" + url.QueryEscape(continuationToken)
	if offsetTokenMs > 0 {
		endpoint += "&offsetToken=" + fmt.Sprintf("%d", offsetTokenMs)
	}
	if requestId != "" {
		endpoint += "&requestId=" + url.QueryEscape(requestId)
	}
	var bodyReader *bytes.Reader
	if useGzip {
		var gzBuf bytes.Buffer
		gz := gzip.NewWriter(&gzBuf)
		_, _ = gz.Write(ndjson)
		_ = gz.Close()
		bodyReader = bytes.NewReader(gzBuf.Bytes())
		compressedLen = gzBuf.Len()
	} else {
		bodyReader = bytes.NewReader(ndjson)
		compressedLen = len(ndjson)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bodyReader)
	if err != nil {
		return "", -1, nil, 0, "", err
	}
	req.Header.Set("Authorization", "Bearer "+patToken)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "PROGRAMMATIC_ACCESS_TOKEN")
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Accept", "application/json")
	/*
		if sessionHeader != "" {
			req.Header.Set("x-snowflake-session", sessionHeader)
		}
	*/
	if useGzip {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", -2, nil, compressedLen, "", err
	}
	defer resp.Body.Close()
	respBody, _ = io.ReadAll(resp.Body)
	statusCode = resp.StatusCode
	sessionHeaderOut = resp.Header.Get("x-snowflake-session")
	if statusCode >= 200 && statusCode < 300 {
		var rr struct {
			NextContinuationToken string `json:"next_continuation_token"`
		}
		_ = json.Unmarshal(respBody, &rr)
		nextContinuation = rr.NextContinuationToken
		return nextContinuation, statusCode, respBody, compressedLen, sessionHeaderOut, nil
	}
	return "", statusCode, respBody, compressedLen, sessionHeaderOut, fmt.Errorf("rows non-2xx: %d", statusCode)
}

func bulkChannelStatus(ctx context.Context, client *http.Client, patToken, ingestHost, database, schema, pipe string, channelNames []string) (bulkStatusResponse, int, []byte, error) {
	var out bulkStatusResponse
	monitorURL := fmt.Sprintf("https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s:bulk-channel-status",
		ingestHost, url.PathEscape(database), url.PathEscape(schema), url.PathEscape(pipe))
	payload := struct {
		ChannelNames []string `json:"channel_names"`
	}{ChannelNames: channelNames}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, monitorURL, bytes.NewReader(b))
	if err != nil {
		return out, 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+patToken)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "PROGRAMMATIC_ACCESS_TOKEN")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return out, 0, nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		_ = json.Unmarshal(body, &out)
		return out, resp.StatusCode, body, nil
	}
	return out, resp.StatusCode, body, fmt.Errorf("status non-2xx: %d", resp.StatusCode)
}

func dropChannel(ctx context.Context, client *http.Client, patToken, channelURL string) error {
	u, err := url.Parse(channelURL)
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("requestId", uuid.NewString())
	u.RawQuery = q.Encode()
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(dctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+patToken)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "PROGRAMMATIC_ACCESS_TOKEN")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("channel delete non-2xx: %s body=%s", resp.Status, string(b))
	}
	return nil
}
