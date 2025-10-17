<h1 align="center">Central Messaging Queue Minus One</h1>
<h6 align="center">In-memory mock message queue for testing</h6>

<p align="center">
  <img alt="GitHub Actions Workflow Status" src="https://img.shields.io/github/actions/workflow/status/1995parham-learning/cmq-1/ci.yaml?style=for-the-badge&logo=github">
  <img alt="GitHub repo size" src="https://img.shields.io/github/repo-size/1995parham-learning/cmq-1?logo=github&style=for-the-badge">
  <img alt="GitHub go.mod Go version (subdirectory of monorepo)" src="https://img.shields.io/github/go-mod/go-version/1995parham-learning/cmq-1?style=for-the-badge&logo=go">
  <img alt="Codecov" src="https://img.shields.io/codecov/c/github/1995parham-learning/cmq-1?logo=codecov&style=for-the-badge">
</p>

## Introduction

CMQ-1 is a lightweight, in-memory mock message queue implementation for testing Go applications. It provides two message delivery semantics inspired by NATS:

- **At-most-once (Subscribers)**: Fast, channel-based delivery where messages may be dropped if the subscriber is slow
- **At-least-once (Consumers)**: Persistent stream-based delivery with offset tracking for guaranteed message delivery

**Key Features:**

- Zero external dependencies (Go standard library only)
- Thread-safe operations with proper synchronization
- Generic types support for type-safe message handling
- Context-based lifecycle management
- Configurable polling intervals for consumers
- Topic-based message routing with stream filtering

## Installation

```bash
go get github.com/1995parham-learning/cmq-1
```

## Quick Start

### Subscriber (At-Most-Once)

```go
import (
    "context"
    "github.com/1995parham-learning/cmq-1/pkg/cmq"
)

// Create a message queue
mmq := cmq.NewMockMessageQueue[string]()

// Subscribe to a topic
sub := mmq.Subscribe("notifications")

// Publish a message
mmq.Publish("notifications", "Hello, World!")

// Fetch messages
ctx := context.Background()
msg, err := sub.Fetch(ctx)
if err != nil {
    // Handle error (e.g., context cancelled)
}
```

### Consumer (At-Least-Once)

```go
import (
    "context"
    "time"
    "github.com/1995parham-learning/cmq-1/pkg/cmq"
    "github.com/1995parham-learning/cmq-1/pkg/stream"
)

// Create a message queue
mmq := cmq.NewMockMessageQueue[int]()

// Create a stream that filters messages for specific topics
str, err := stream.New[int]("my-stream", []string{"events", "alerts"})
if err != nil {
    panic(err)
}

// Register the stream with the queue
if err := mmq.Stream(str); err != nil {
    panic(err)
}

// Publish some messages
mmq.Publish("events", 1)
mmq.Publish("alerts", 2)
mmq.Publish("other", 3) // Won't be stored in the stream

// Create a consumer for the stream
consumer, err := mmq.Consume("my-stream")
if err != nil {
    panic(err)
}

// Start the consumer (spawns background goroutine)
consumer.Start()
defer consumer.Stop()

// Wait for messages to arrive
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := consumer.Wait(ctx); err != nil {
    panic(err)
}

// Fetch messages
for {
    msg, ok := consumer.Fetch()
    if !ok {
        break
    }
    println(msg)
}
```

## Advanced Usage

### Custom Polling Interval

By default, consumers poll the stream every second. You can customize this:

```go
import "github.com/1995parham-learning/cmq-1/pkg/consumer"

// Create consumer with 100ms polling interval
str, _ := stream.New[string]("fast-stream", []string{"events"})
c := consumer.NewWithInterval(str, 100*time.Millisecond)
```

### Context-Based Consumer Lifecycle

Instead of manually calling `Stop()`, use context-based lifecycle:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

consumer.StartWithContext(ctx)
// Consumer automatically stops when context is cancelled
```

### Multiple Consumers on Same Stream

Multiple consumers can read from the same stream independently:

```go
str, _ := stream.New[int]("shared", []string{"topic"})
mmq.Stream(str)

con1, _ := mmq.Consume("shared")
con2, _ := mmq.Consume("shared")

con1.Start()
con2.Start()
// Both consumers maintain independent offsets
```

## API Reference

### CMQ Interface

```go
type CMQ[T any] interface {
    Publish(topic string, message T)
    Subscribe(topic string) subscriber.Subscriber[T]
    Consume(stream string) (*consumer.Consumer[T], error)
    Stream(stream *stream.Stream[T]) error
}
```

### Consumer Methods

- `Start()` - Begin background polling (idempotent)
- `StartWithContext(ctx)` - Begin polling with context-based cancellation
- `Stop()` - Stop background polling (safe to call multiple times)
- `Wait(ctx)` - Block until messages are available (reusable)
- `Fetch()` - Retrieve next message from queue

### Subscriber Methods

- `Fetch(ctx)` - Block until message arrives or context cancelled

### Stream Methods

- `New[T](name, topics)` - Create stream with topic filters
- `Insert(message)` - Add message to stream
- `Fetch(index)` - Retrieve message at offset
- `Has(topic)` - Check if stream filters for topic

## Error Handling

```go
var (
    ErrDuplicateStream = errors.New("stream with the same name already exists")
    ErrStreamNotFound  = errors.New("stream with given name does not exist")
    ErrIndexOutOfRange = errors.New("index out of range")
    ErrEmptyTopics     = errors.New("topics list cannot be empty")
)
```

## Design Decisions

### At-Most-Once vs At-Least-Once

**Subscribers (At-Most-Once):**

- Use unbuffered channels by default
- Messages are dropped if subscriber is slow
- Similar to NATS pub-sub behavior
- Best for non-critical notifications

**Consumers (At-Least-Once):**

- Messages stored persistently in streams
- Each consumer maintains its own offset
- Polling-based retrieval (configurable interval)
- Best for critical event processing

### Thread Safety

All operations are protected by appropriate locks:

- `RWMutex` for read-heavy operations (message queue, streams)
- `Mutex` for consumer state
- Non-blocking channel operations with `select`

### Generic Types

All components use Go 1.18+ generics for type safety:

```go
mmq := cmq.NewMockMessageQueue[MyStruct]()
```

## Testing

```bash
# Run tests
go test ./...

# Run with coverage
go test -cover ./...

# Run with race detector
go test -race ./...
```

## Contributing

This is a learning project. Feel free to:

- Report issues
- Suggest improvements
- Submit pull requests

## License

See [LICENSE](LICENSE) file for details.

## Acknowledgments

Inspired by [NATS](https://nats.io/) messaging system concepts.
