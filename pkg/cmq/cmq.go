package cmq

import (
	"errors"
	"sync"

	"github.com/1995parham-learning/cmq-1/pkg/consumer"
	"github.com/1995parham-learning/cmq-1/pkg/stream"
	"github.com/1995parham-learning/cmq-1/pkg/subscriber"
)

var (
	ErrDuplicateStream = errors.New("stream with a same name already exists")
	ErrStreamNotFound = errors.New("stream with given name does not exist")
)

type CMQ[T any] interface {
	Publish(topic string, message T)
	Subscribe(topic string) subscriber.Subscriber[T]
	Consume(stream string) (consumer.Consumer[T], error)
	Stream(stream *stream.Stream[T])
}

type MockCMQ[T any] struct {
	subscribers map[string][]chan<- T
	streams     map[string]*stream.Stream[T]

	lock sync.RWMutex
}

func NewMockMessageQueue[T any]() *MockCMQ[T] {
	return &MockCMQ[T]{
		subscribers: make(map[string][]chan<- T),
		streams: make(map[string]*stream.Stream[T]),
	}
}

func (mmq *MockCMQ[T]) Stream(stream *stream.Stream[T]) error {
	mmq.lock.Lock()
	defer mmq.lock.Unlock()

	if _, ok := mmq.streams[stream.Name]; ok {
		return ErrDuplicateStream
	}

	mmq.streams[stream.Name] = stream

	return nil
}

// Define new consumer with at least once semantic.
func (mmq *MockCMQ[T]) Consume(stream string) (*consumer.Consumer[T], error){
	str, ok := mmq.streams[stream]
	if !ok {
		return nil, ErrStreamNotFound
	}


	return consumer.New(str), nil
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
