// Package cmq provides an in-memory mock message queue implementation for testing.
// It supports both at-most-once (subscriber) and at-least-once (consumer) message
// delivery semantics, making it suitable for testing applications that depend on
// message queues without requiring external dependencies.
package cmq

import (
	"errors"
	"sync"

	"github.com/1995parham-learning/cmq-1/pkg/consumer"
	"github.com/1995parham-learning/cmq-1/pkg/stream"
	"github.com/1995parham-learning/cmq-1/pkg/subscriber"
)

var (
	ErrDuplicateStream = errors.New("stream with the same name already exists")
	ErrStreamNotFound  = errors.New("stream with given name does not exist")
)

// CMQ is the interface for a mock message queue supporting both
// at-most-once (subscriber) and at-least-once (consumer) semantics.
type CMQ[T any] interface {
	Publish(topic string, message T)
	Subscribe(topic string) subscriber.Subscriber[T]
	Consume(stream string) (*consumer.Consumer[T], error)
	Stream(stream *stream.Stream[T]) error
}

// MockCMQ is an in-memory implementation of the CMQ interface.
// It provides thread-safe message publishing with topic-based routing.
type MockCMQ[T any] struct {
	subscribers map[string][]chan<- T
	streams     map[string]*stream.Stream[T]

	lock sync.RWMutex
}

// NewMockMessageQueue creates a new in-memory message queue.
func NewMockMessageQueue[T any]() *MockCMQ[T] {
	return &MockCMQ[T]{
		subscribers: make(map[string][]chan<- T),
		streams:     make(map[string]*stream.Stream[T]),
	}
}

// Stream registers a stream with the message queue.
// Returns ErrDuplicateStream if a stream with the same name already exists.
func (mmq *MockCMQ[T]) Stream(stream *stream.Stream[T]) error {
	mmq.lock.Lock()
	defer mmq.lock.Unlock()

	if _, ok := mmq.streams[stream.Name]; ok {
		return ErrDuplicateStream
	}

	mmq.streams[stream.Name] = stream

	return nil
}

// Consume creates a new consumer with at-least-once semantic for the given stream.
// Returns ErrStreamNotFound if the stream doesn't exist.
func (mmq *MockCMQ[T]) Consume(stream string) (*consumer.Consumer[T], error) {
	mmq.lock.RLock()
	str, ok := mmq.streams[stream]
	mmq.lock.RUnlock()

	if !ok {
		return nil, ErrStreamNotFound
	}

	return consumer.New(str), nil
}

// Publish sends a message to all subscribers on the given topic and stores it in matching streams.
// Slow subscribers are dropped (at-most-once semantic), but messages are always persisted to streams.
func (mmq *MockCMQ[T]) Publish(topic string, message T) {
	mmq.lock.RLock()
	defer mmq.lock.RUnlock()

	for _, channel := range mmq.subscribers[topic] {
		select {
		case channel <- message:
		default:
			// just like NATS we are ignoring subscribers that cannot consume as fast as
			// producer, but for consumers we have stream in which we store messages.
		}
	}

	// store messages into stream based on their topic filters.
	for _, stream := range mmq.streams {
		if stream.Has(topic) {
			stream.Insert(message)
		}
	}
}

// Subscribe creates a new subscriber with at-most-once semantic on the given topic.
// Messages may be dropped if the subscriber cannot keep up with the producer.
func (mmq *MockCMQ[T]) Subscribe(topic string) subscriber.Subscriber[T] {
	mmq.lock.Lock()
	defer mmq.lock.Unlock()

	ch := make(chan T)

	sub := subscriber.New(ch)

	subscribers := mmq.subscribers[topic]
	mmq.subscribers[topic] = append(subscribers, ch)

	return sub
}
