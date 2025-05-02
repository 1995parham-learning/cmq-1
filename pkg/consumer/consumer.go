package consumer

import (
	"sync"
	"time"

	"github.com/1995parham-learning/cmq-1/pkg/stream"
)

type Consumer[T any] struct {
	stream   *stream.Stream[T]
	offset   int
	messages []T
	close    chan struct{}

	lock sync.Mutex
}

func New[T any](stream *stream.Stream[T]) *Consumer[T] {
	return &Consumer[T]{
		stream:   stream,
		offset:   0,
		messages: nil,
		close:    make(chan struct{}),
		lock:     sync.Mutex{},
	}
}

func (c *Consumer[T]) Wait() {
	for len(c.messages) == 0 {
	}
}

func (c *Consumer[T]) Fetch() (T, bool) {
	if len(c.messages) == 0 {
		return *new(T), false
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	v := c.messages[0]
	c.messages = c.messages[1:]

	return v, true
}

func (c *Consumer[T]) fetch() {
	c.lock.Lock()
	defer c.lock.Unlock()

	v, err := c.stream.Fetch(c.offset)
	if err == nil {
		c.offset += 1
		c.messages = append(c.messages, v)
	}
}

func (c *Consumer[T]) Start() {
	ticker := time.NewTicker(time.Second)

	go func() {
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

func (c *Consumer[T]) Stop() {
	close(c.close)
}
