// Package subscriber provides at-most-once message delivery semantics.
// Subscribers receive messages via channels and may lose messages if they
// cannot consume them fast enough. This is similar to NATS pub-sub behavior.
package subscriber

import "context"

// Subscriber provides at-most-once message delivery semantics.
// Messages may be lost if the subscriber cannot consume them fast enough.
type Subscriber[T any] struct {
	ch <-chan T
}

// New creates a new Subscriber wrapping the given channel.
func New[T any](ch <-chan T) Subscriber[T] {
	return Subscriber[T]{
		ch: ch,
	}
}

// Fetch retrieves the next message from the subscriber.
// Blocks until a message arrives or the context is cancelled.
func (sc Subscriber[T]) Fetch(ctx context.Context) (T, error) {
	select {
	case m := <-sc.ch:
		return m, nil
	case <-ctx.Done():
		return *new(T), ctx.Err()
	}
}
