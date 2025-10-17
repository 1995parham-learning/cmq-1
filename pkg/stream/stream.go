// Package stream provides persistent message storage for topics.
// Streams filter and store messages based on topic subscriptions,
// enabling consumers to retrieve messages with at-least-once guarantees.
package stream

import (
	"errors"
	"slices"
	"sync"
)

var (
	ErrIndexOutOfRange = errors.New("index out of range")
	ErrEmptyTopics     = errors.New("topics list cannot be empty")
)

// Stream stores messages for given topics.
// Streams are used by message queue to store messages.
type Stream[T any] struct {
	Name     string
	topics   []string
	messages []T
	lock     sync.RWMutex
}

// New creates a new Stream that filters messages for the specified topics.
// Returns an error if topics is nil or empty.
func New[T any](name string, topics []string) (*Stream[T], error) {
	if len(topics) == 0 {
		return nil, ErrEmptyTopics
	}

	return &Stream[T]{
		Name:     name,
		topics:   topics,
		messages: nil,
	}, nil
}

// Insert appends a message to the stream.
func (s *Stream[T]) Insert(message T) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.messages = append(s.messages, message)
}

// Fetch retrieves the message at the given index.
// Returns ErrIndexOutOfRange if the index is invalid.
func (s *Stream[T]) Fetch(index int) (T, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if index < 0 || index >= len(s.messages) {
		return *new(T), ErrIndexOutOfRange
	}

	return s.messages[index], nil
}

// Has checks if the stream filters for the given topic.
func (s *Stream[T]) Has(topic string) bool {
	return slices.Contains(s.topics, topic)
}
