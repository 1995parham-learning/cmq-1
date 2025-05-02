package subscriber

import "context"

type Subscriber[T any] struct {
	ch   <-chan T
}

func New[T any](ch <-chan T) Subscriber[T]{
	return Subscriber[T]{
		ch: ch,
	}
}

func (sc Subscriber[T]) Fetch(ctx context.Context) (T, error) {
	select {
	case m := <-sc.ch:
		return m, nil
	case <-ctx.Done():
		return *new(T), ctx.Err()
	}
}
