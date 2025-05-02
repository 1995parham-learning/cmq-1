package stream

import "sync"

// Stream stores messages for given topics.
// Streams are used by message queue to store mesages.
type Stream[T any] struct {
	name     string
	topics   []string
	messages []T
	seqNum   int
	lock     sync.Mutex
}

func (s *Stream[T]) Insert(message T) {
	s.lock.Lock()

	s.messages = append(s.messages, message)
	s.seqNum += 1

	defer s.lock.Unlock()
}
