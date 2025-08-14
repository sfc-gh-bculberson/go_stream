go_stream
====================

A Go program that generates fake ski lift tickets and streams them to Snowflake Streaming Ingest.

Features
- Producer/consumer with buffered channel
- Faker-generated names and addresses
- Batching with line-delimited JSON rows
- Snowflake control-plane discovery and channel lifecycle (open/rows/close)
- Configurable throughput (events per second)

Requirements
- Go 1.22+ installed

Build
```bash
go build
```

Run
Set required Snowflake info and run the binary. Flags have ENV var equivalents.

Required:
- ACCOUNT (or -account)
- JWT (or -jwt) — Snowflake key pair JWT
- DATABASE (or -database)
- SCHEMA (or -schema)
- PIPE (or -pipe)

Optional:
- EPS (or -eps) — events per second; default 140
- BATCH (or -batch) — rows batch size; default 100
- COUNT (or -count) — total events to produce; 0 for infinite; default 100
- BUFFER (or -buffer) — channel buffer size; default 100

Example
```bash
ACCOUNT=acme_xyz \
JWT="<your_keypair_jwt>" \
DATABASE=MYDB \
SCHEMA=PUBLIC \
PIPE=MY_PIPE \
EPS=120 \
BATCH=200 \
./go_stream -count=1000
```

What it does
1. Resolve ingest host via Snowflake control-plane:
   - GET https://{ACCOUNT}.snowflakecomputing.com/v2/streaming/hostname
2. Open a channel (random name per run):
   - PUT https://{INGEST_HOST}/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}
   - Send an empty JSON body `{}`
   - Read `next_continuation_token`
3. Generate lift ticket events at `EPS` and batch them.
4. Send rows in batches:
   - POST https://{INGEST_HOST}/v2/streaming/data/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}/rows?continuationToken={token}
   - Body is a string of newline-delimited JSON objects (each event), not an array.
5. On exit or error, close the channel:
   - DELETE https://{INGEST_HOST}/v2/streaming/databases/{db}/schemas/{schema}/pipes/{pipe}/channels/{channel}

Event shape (uppercase keys)
Each line in the rows body is a single JSON object with uppercase keys, for example:
```json
{"TXID":"...","RFID":"0x...","RESORT":"...","PURCHASE_TIME":"...","EXPIRATION_TIME":"...","DAYS":3,"NAME":"...","ADDRESS":{"STREET_ADDRESS":"...","CITY":"...","STATE":"...","POSTALCODE":"..."},"PHONE":"...","EMAIL":"...","EMERGENCY_CONTACT":{"NAME":"...","PHONE":"..."}}
```

Tuning
- Throughput: -eps / EPS
- Batching: -batch / BATCH
- Total events: -count / COUNT

Logging
- Logs control/ingest host resolution and channel token
- Logs each rows request body (newline-delimited JSON string)

Troubleshooting
- Context canceled on rows: EPS too high vs. ingest; increase rows timeout or lower EPS/batch.
- Non-2xx responses: the program logs HTTP status and response body for control, open, rows, and delete calls.

Credits
- Fake data via `github.com/go-faker/faker/v4`


Deploy to Kubernetes
--------------------

Using Helm chart in `charts/go-stream`.

1) Prepare values from your shell env.
```bash
envsubst < charts/go-stream/values.from-env.tpl.yaml > charts/go-stream/values.yaml
```
2) Install/upgrade:
```bash
helm upgrade --install go-stream ./charts/go-stream
```