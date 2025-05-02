package stream

import "sync"

// Stream stores messages for given topics.
// Streams are used by message queue to store mesages.
type Stream[T any] struct {
	name     string
	topics   []string
	messages []T
	seqNum   int
	lock     sync.RWMutex
}

func (s *Stream[T]) Insert(message T) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.messages = append(s.messages, message)
	s.seqNum += 1
}

func (s *Stream[T]) Fetch(index int) T {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.messages[index]
}
