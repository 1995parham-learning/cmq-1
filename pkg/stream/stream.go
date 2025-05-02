package stream

import (
	"errors"
	"slices"
	"sync"
)

var (
	ErrIndexOutOfRange = errors.New("index of out range")
)

// Stream stores messages for given topics.
// Streams are used by message queue to store mesages.
type Stream[T any] struct {
	name     string
	topics   []string
	messages []T
	seqNum   int
	lock     sync.RWMutex
}

func New[T any](name string, topics []string) *Stream[T] {
	return &Stream[T]{
		name: name,
		topics: topics,
		messages: nil,
		lock: sync.RWMutex{},
	}
}

func (s *Stream[T]) Insert(message T) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.messages = append(s.messages, message)
}

func (s *Stream[T]) Fetch(index int) (T, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if index >= len(s.messages) {
		return *new(T), ErrIndexOutOfRange
	}

	return s.messages[index], nil
}

func (s *Stream[T]) Has(topic string) bool {
	return slices.Contains(s.topics, topic)
}
