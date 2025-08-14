FROM golang:1.25.0-alpine AS builder

WORKDIR /app
COPY . .
RUN go mod download
RUN go mod verify

RUN CGO_ENABLED=0 GOOS=linux go build -o go_stream .

FROM alpine:latest

WORKDIR /app
COPY --from=builder /app/go_stream /app/go_stream

ENTRYPOINT ["/app/go_stream"]