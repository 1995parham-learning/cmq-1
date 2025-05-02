package subscriber_test

import (
	"context"
	"testing"
	"time"

	"github.com/1995parham-learning/cmq-1/pkg/subscriber"
	"github.com/stretchr/testify/require"
)

func TestFetch(t *testing.T) {
	require := require.New(t)
	ch := make(chan string)

	sub := subscriber.New(ch)

	go func() {
		time.Sleep(time.Second)
		ch <- "Elahe Dastan"
	}()

	v, err := sub.Fetch(context.Background())
	require.NoError(err)
	require.Equal(v, "Elahe Dastan")
}

func TestFetchTimeout(t *testing.T) {
	require := require.New(t)
	ch := make(chan string)

	sub := subscriber.New(ch)

	ctx := context.Background()
	ctx, done := context.WithTimeout(ctx, time.Millisecond)
	defer done()

	go func() {
		time.Sleep(time.Second)
		ch <- "Elahe Dastan"
	}()

	v, err := sub.Fetch(ctx)
	require.ErrorIs(err, context.DeadlineExceeded)
	require.Equal(v, "")
}
