// Package consumer provides at-least-once message delivery semantics.
// Consumers maintain an offset into a stream and poll for new messages at
// regular intervals, ensuring messages are not lost even if the consumer
// cannot keep up with the producer.
package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/1995parham-learning/cmq-1/pkg/stream"
)

// Consumer provides at-least-once message delivery semantics by maintaining
// an offset into a stream and polling for new messages at regular intervals.
type Consumer[T any] struct {
	stream   *stream.Stream[T]
	offset   int
	messages []T
	close    chan struct{}
	ready    chan struct{}
	interval time.Duration
	started  bool

	lock sync.Mutex
}

// New creates a new Consumer for the given stream with a default 1-second polling interval.
func New[T any](stream *stream.Stream[T]) *Consumer[T] {
	return &Consumer[T]{
		stream:   stream,
		offset:   0,
		messages: nil,
		close:    make(chan struct{}),
		ready:    make(chan struct{}, 1), // Buffered to allow signaling even if no one is waiting
		interval: time.Second,
		started:  false,
	}
}

// NewWithInterval creates a new Consumer with a custom polling interval.
func NewWithInterval[T any](stream *stream.Stream[T], interval time.Duration) *Consumer[T] {
	c := New(stream)
	c.interval = interval
	return c
}

// Wait blocks until at least one message is available or context is cancelled.
// Returns an error if the context is cancelled before a message arrives.
// This method can be called multiple times - it checks if messages are already available.
func (c *Consumer[T]) Wait(ctx context.Context) error {
	// Check if messages are already available
	c.lock.Lock()
	hasMessages := len(c.messages) > 0
	c.lock.Unlock()

	if hasMessages {
		return nil
	}

	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Fetch retrieves the next available message from the consumer's queue.
// Returns the message and true if available, or zero value and false if queue is empty.
func (c *Consumer[T]) Fetch() (T, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.messages) == 0 {
		return *new(T), false
	}

	v := c.messages[0]
	c.messages = c.messages[1:]

	return v, true
}

func (c *Consumer[T]) fetch() {
	c.lock.Lock()
	defer c.lock.Unlock()

	v, err := c.stream.Fetch(c.offset)
	if err == nil {
		c.offset++
		wasEmpty := len(c.messages) == 0
		c.messages = append(c.messages, v)

		// Signal waiting goroutines when messages become available
		if wasEmpty {
			select {
			case c.ready <- struct{}{}:
			default:
				// Channel already has a signal pending
			}
		}
	}
}

// Start begins the background polling goroutine that fetches messages from the stream.
// Must be called before messages can be consumed. Always pair with Stop() to avoid goroutine leaks.
// Calling Start multiple times is safe - only the first call will start the goroutine.
func (c *Consumer[T]) Start() {
	c.lock.Lock()
	if c.started {
		c.lock.Unlock()
		return
	}
	c.started = true
	c.lock.Unlock()

	ticker := time.NewTicker(c.interval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.fetch()
			case <-c.close:
				return
			}
		}
	}()
}

// StartWithContext begins the background polling with context support.
// The consumer will automatically stop when the context is cancelled.
// Calling StartWithContext multiple times is safe - only the first call will start the goroutine.
func (c *Consumer[T]) StartWithContext(ctx context.Context) {
	c.lock.Lock()
	if c.started {
		c.lock.Unlock()
		return
	}
	c.started = true
	c.lock.Unlock()

	ticker := time.NewTicker(c.interval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.fetch()
			case <-c.close:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop terminates the background polling goroutine.
// It's safe to call multiple times.
func (c *Consumer[T]) Stop() {
	select {
	case <-c.close:
		// Already closed
		return
	default:
		close(c.close)
	}
}
