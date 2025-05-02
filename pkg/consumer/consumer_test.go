package consumer_test

import (
	"testing"
	"time"

	"github.com/1995parham-learning/cmq-1/pkg/consumer"
	"github.com/1995parham-learning/cmq-1/pkg/stream"
	"github.com/stretchr/testify/require"
)

func TestConsumerFetch(t *testing.T) {
	require := require.New(t)

	str := stream.New[string]("rides", []string{"ride.accepted"})
	con := consumer.New(str)

	str.Insert("ride 73 is accepted by driver 78")

	{
		v, ok := con.Fetch()
		require.False(ok)
		require.Equal(v, "")
	}

	con.Start()
	defer con.Stop()

	time.Sleep(2 * time.Second)

	{
		v, ok := con.Fetch()
		require.True(ok)
		require.Equal(v, "ride 73 is accepted by driver 78")
	}
}
