package cmq_test

import (
	"context"
	"testing"
	"time"

	"github.com/1995parham-learning/cmq-1/pkg/cmq"
	"github.com/1995parham-learning/cmq-1/pkg/stream"
	"github.com/stretchr/testify/require"
)

func TestSubscriber(t *testing.T) {
	require := require.New(t)

	mmq := cmq.NewMockMessageQueue[string]()

	sub := mmq.Subscribe("hello")

	done := make(chan struct{})
	ready := make(chan struct{})
	var receivedMsg string
	var receivedErr error

	go func() {
		close(ready) // Signal that we're about to call Fetch
		receivedMsg, receivedErr = sub.Fetch(context.Background())
		close(done)
	}()

	<-ready                           // Wait for goroutine to be ready
	time.Sleep(10 * time.Millisecond) // Small delay to ensure Fetch is blocking
	mmq.Publish("hello", "Hello World")

	<-done
	require.NoError(receivedErr)
	require.Equal("Hello World", receivedMsg)
}

func TestSubscriberTimeout(t *testing.T) {
	require := require.New(t)

	mmq := cmq.NewMockMessageQueue[string]()

	sub := mmq.Subscribe("hello")

	ctx := context.Background()
	ctx, done := context.WithTimeout(ctx, time.Second)
	defer done()

	v, err := sub.Fetch(ctx)
	require.Error(err)
	require.Equal(v, "")
}

func TestConsumer(t *testing.T) {
	require := require.New(t)

	mmq := cmq.NewMockMessageQueue[int]()

	str, err := stream.New[int]("hello", []string{"hello"})
	require.NoError(err)
	require.NoError(mmq.Stream(str))

	mmq.Publish("hello", 1378)

	con, err := mmq.Consume("hello")
	require.NoError(err)

	con.Start()
	defer con.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	require.NoError(con.Wait(ctx))

	v, ok := con.Fetch()
	require.True(ok)
	require.Equal(v, 1378)
}

func TestDuplicateStream(t *testing.T) {
	require := require.New(t)

	mmq := cmq.NewMockMessageQueue[int]()

	str1, err := stream.New[int]("test", []string{"topic1"})
	require.NoError(err)

	str2, err := stream.New[int]("test", []string{"topic2"})
	require.NoError(err)

	require.NoError(mmq.Stream(str1))
	err = mmq.Stream(str2)
	require.ErrorIs(err, cmq.ErrDuplicateStream)
}

func TestConsumeNonExistentStream(t *testing.T) {
	require := require.New(t)

	mmq := cmq.NewMockMessageQueue[int]()

	_, err := mmq.Consume("nonexistent")
	require.ErrorIs(err, cmq.ErrStreamNotFound)
}

func TestMultipleConsumersOnSameStream(t *testing.T) {
	require := require.New(t)

	mmq := cmq.NewMockMessageQueue[int]()

	str, err := stream.New[int]("numbers", []string{"num"})
	require.NoError(err)
	require.NoError(mmq.Stream(str))

	mmq.Publish("num", 1)
	mmq.Publish("num", 2)
	mmq.Publish("num", 3)

	con1, err := mmq.Consume("numbers")
	require.NoError(err)
	con1.Start()
	defer con1.Stop()

	con2, err := mmq.Consume("numbers")
	require.NoError(err)
	con2.Start()
	defer con2.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	require.NoError(con1.Wait(ctx))
	require.NoError(con2.Wait(ctx))

	// Both consumers should get the same messages (independent offsets)
	v1, ok1 := con1.Fetch()
	require.True(ok1)
	require.Equal(1, v1)

	v2, ok2 := con2.Fetch()
	require.True(ok2)
	require.Equal(1, v2)
}

func TestEmptyStreamTopics(t *testing.T) {
	require := require.New(t)

	_, err := stream.New[int]("empty", []string{})
	require.ErrorIs(err, stream.ErrEmptyTopics)

	_, err = stream.New[int]("nil", nil)
	require.ErrorIs(err, stream.ErrEmptyTopics)
}
