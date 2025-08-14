FROM golang:latest

WORKDIR /app
COPY . .
RUN go mod download
RUN go mod verify

RUN go build -o go_stream .

ENTRYPOINT ["/app/go_stream"]