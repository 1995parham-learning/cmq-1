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

	go func() {
		v, err := sub.Fetch(context.Background())
		require.NoError(err)
		require.Equal(v, "Hello World")
	}()

	mmq.Publish("hello", "Hello World")
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

	require.NoError(mmq.Stream(stream.New[int]("hello", []string{"hello"})))

	mmq.Publish("hello", 1378)

	con, err := mmq.Consume("hello")
	require.NoError(err)

	con.Start()
	defer con.Stop()

	con.Wait()

	v, ok := con.Fetch()
	require.True(ok)
	require.Equal(v, 1378)
}
