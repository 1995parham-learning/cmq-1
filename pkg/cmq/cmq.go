package cmq

import (
	"errors"
	"fmt"
)

var (
	ErrDuplicateSubscriber = errors.New("subscriber on the topic with a same name exists")
	ErrTopicNotFound       = errors.New("topic doesn't exist")
	ErrSubscriberNotFound  = errors.New("subscriber doesn't exist")
)

type SubscriberFullError struct {
	Subscribers []string
	Topic       string
}

func (err SubscriberFullError) Error() string {
	return fmt.Sprintf(
		"cannot publish into subscriber named %s of topic %s because its channel is already full",
		err.Subscribers,
		err.Topic,
	)
}

type SubscriberContext[T any] struct {
	ch   chan T
	exit chan struct{}
}

func (sc SubscriberContext[T]) Channel() <-chan T {
	return sc.ch
}

func (sc SubscriberContext[T]) Close() {
	sc.exit <- struct{}{}
	<-sc.exit
	close(sc.ch)
}

type MessageQueue[T any] interface {
	Publish(topic string, message T) error
	Register(name, topic string, size int) error
	Subscribe(subscriber string, topic string) (<-chan T, error)
}

type MockMessageQueue[T any] struct {
	queues map[string]map[string]chan T
}

func NewMockMessageQueue[T any]() MockMessageQueue[T] {
	return MockMessageQueue[T]{
		queues: make(map[string]map[string]chan T),
	}
}

func (mmq MockMessageQueue[T]) Publish(topic string, message T) error {
	if _, ok := mmq.queues[topic]; !ok {
		mmq.queues[topic] = make(map[string]chan T)
	}

	fullSubscribers := make([]string, 0)

	for name, channel := range mmq.queues[topic] {
		select {
		case channel <- message:
		default:
			// just like NATS we are ignoring consumers that can not consume as fast as
			// producer.
			fullSubscribers = append(fullSubscribers, name)
		}
	}

	if len(fullSubscribers) == 0 {
		return nil
	} else {
		return SubscriberFullError{
			Subscribers: fullSubscribers,
			Topic:       topic,
		}
	}
}

// Register a subscribe group on a topic. You need to register subscriber group before using it.
// Subscriber groups start getting messages just after the registeration and only stores "size"
// number of them before dropping them out.
func (mmq MockMessageQueue[T]) Register(subscriber string, topic string, size int) error {
	if _, ok := mmq.queues[topic]; !ok {
		mmq.queues[topic] = make(map[string]chan T)
	}

	if _, ok := mmq.queues[topic][subscriber]; !ok {
		mmq.queues[topic][subscriber] = make(chan T, size)
	} else {
		return ErrDuplicateSubscriber
	}

	return nil
}

// Subscribe using subscriber group on a given topic. In subscriber group, each subscriber
// receives a message at most once. You need to close the subscribe using the context to
// prevent go routine leak.
func (mmq MockMessageQueue[T]) Subscribe(subscriber string, topic string) (SubscriberContext[T], error) {
	ch := make(chan T)

	sc := SubscriberContext[T]{
		ch:   ch,
		exit: make(chan struct{}),
	}

	if _, ok := mmq.queues[topic]; !ok {
		sc.Close()

		return sc, ErrTopicNotFound
	}

	if _, ok := mmq.queues[topic][subscriber]; !ok {
		sc.Close()

		return sc, ErrSubscriberNotFound
	}

	go func() {
		defer func() {
			close(sc.exit)
		}()

		for {
			select {
			case <-sc.exit:
				return

			case msg := <-mmq.queues[topic][subscriber]:
				select {
				case ch <- msg:
				case <-sc.exit:
					return
				}
			}
		}
	}()

	return sc, nil
}
