package consumer

import (
	"sync"

	"github.com/1995parham-learning/cmq-1/pkg/stream"
)

type Consumer[T any] struct {
	stream *stream.Stream[T]
	topics []string
	offset int

	lock sync.Mutex
}

func (c *Consumer[T]) Fetch() T {
  c.lock.Lock()
	defer c.lock.Unlock()

	v, err := c.stream.Fetch(c.offset)
	if err != nil {
		return v
	}

	c.offset += 1

	return v
}
