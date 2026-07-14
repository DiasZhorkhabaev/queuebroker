# Queue Broker

A concurrent HTTP queue broker implemented in Go.

## Features

- FIFO message delivery
- Multiple named queues
- Concurrent-safe implementation
- Long polling with timeout support
- Thread-safe using sync.Mutex
- Waiting clients handled through Go channels

## API

### Put message

```http
PUT /{queue}?v=value
```

Example:

```http
PUT /pet?v=cat
```

Response:

```
200 OK
```

---

### Get message

```http
GET /{queue}
```

Response:

```
cat
```

or

```
404 Not Found
```

---

### Long polling

```http
GET /{queue}?timeout=10
```

The request waits up to 10 seconds for a message.

If a message arrives before the timeout, it is returned immediately.

Otherwise:

```
404 Not Found
```

## Project structure

```
.
├── main.go
└── go.mod
```

## Technologies

- Go
- net/http
- sync.Mutex
- Goroutines
- Channels

## Run

```bash
go run .
```

The server starts on port:

```
8080
```

Examples:

```
PUT  /pet?v=cat
GET  /pet
GET  /pet?timeout=10
```
