package cmq

import (
	"errors"
	"sync"

	"github.com/1995parham-learning/cmq-1/pkg/stream"
	"github.com/1995parham-learning/cmq-1/pkg/subscriber"
)

type CMQ[T any] interface {
	Publish(topic string, message T)
	Subscribe(topic string) subscriber.Subscriber[T]
	Consume(stream string, topics []string)
}

var (
	ErrDuplicateSubscriber = errors.New("subscriber on the topic with a same name exists")
	ErrTopicNotFound       = errors.New("topic doesn't exist")
	ErrSubscriberNotFound  = errors.New("subscriber doesn't exist")
)

type MockCMQ[T any] struct {
	subscribers map[string][]chan<- T
	streams     []*stream.Stream[T]

	lock sync.RWMutex
}

func NewMockMessageQueue[T any]() *MockCMQ[T] {
	return &MockCMQ[T]{
		subscribers: make(map[string][]chan<- T),
	}
}

func (mmq *MockCMQ[T]) Publish(topic string, message T) {
	mmq.lock.RLock()
	defer mmq.lock.RUnlock()

	for _, channel := range mmq.subscribers[topic] {
		select {
		case channel <- message:
		default:
			// just like NATS we are ignoring subscribers that can not consume as fast as
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

// Subscribe with at-most once semantic on a given topic. In subscriber group, each subscriber
// receives a message at most once.
func (mmq *MockCMQ[T]) Subscribe(topic string) subscriber.Subscriber[T] {
	mmq.lock.Lock()
	defer mmq.lock.Unlock()

	ch := make(chan T)

	sub := subscriber.New(ch)

	subscribers := mmq.subscribers[topic]
	mmq.subscribers[topic] = append(subscribers, ch)

	return sub
}
