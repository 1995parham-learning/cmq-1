package cmq_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/1995parham-learning/cmq-1/pkg/cmq"
)

func TestFullSubscriber(t *testing.T) {
	mmq := cmq.NewMockMessageQueue[int]()

	// create a subscribe groip on "numbers" topic which is named "s1"
	if err := mmq.Register("s1", "numbers", 1); err != nil {
		t.Fatalf("failed to create subscriber group named s1 on numbers topic %s", err)
	}

	if err := mmq.Publish("numbers", 78); err != nil {
		t.Fatalf("failed to publish on numbers topic %s", err)
	}

	err := mmq.Publish("numbers", 78)
	if err == nil {
		t.Fatalf("publish on full subscriber should fail")
	}

	var subErr cmq.SubscriberFullError
	if !errors.As(err, &subErr) {
		t.Fatalf("publish on full subscriber should fail with subscribe error but failed with %s", err)
	}
	if subErr.Topic != "numbers" {
		t.Fatalf("the name of topic is %s instead of numbers", subErr.Topic)
	}
	if subErr.Subscriber != "s1" {
		t.Fatalf("the name of subscriber is %s instead of s1", subErr.Subscriber)
	}
}

func TestPublishAndSubscribe(t *testing.T) {
	mmq := cmq.NewMockMessageQueue[int]()

	// create a subscribe groip on "numbers" topic which is named "s1"
	if err := mmq.Register("s1", "numbers", 10); err != nil {
		t.Fatalf("failed to create subscriber group named s1 on numbers topic %s", err)
	}

	sub, err := mmq.Subscribe("s1", "numbers")
	if err != nil {
		t.Fatalf("failed to subscribe on group named s1 and numbers topic %s", err)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		i := <-sub.Channel()
		if i != 78 {
			t.Errorf("read %d from subscribe instead of 78", i)
			t.Fail()
		}
		sub.Close()
		wg.Done()
	}()

	// subscriber has 10 empty place, so we can insert
	// 10 numbers without any error.
	if err := mmq.Publish("numbers", 78); err != nil {
		t.Fatalf("failed to publish on numbers topic %s", err)
	}
	for i := range 9 {
		if err := mmq.Publish("numbers", i); err != nil {
			t.Fatalf("failed to publish on numbers topic %s", err)
		}
	}

	wg.Wait()
}
