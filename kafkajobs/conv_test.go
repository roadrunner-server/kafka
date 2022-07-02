package amqpjobs

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestConv(t *testing.T) {
	table := amqp.Table{}
	table["a"] = 1
	table["b"] = int8(1)
	table["c"] = int16(1)
	table["d"] = int32(1)
	table["e"] = int64(1)

	table["f"] = uint(1)
	table["j"] = uint8(1)
	table["i"] = uint16(1)
	table["g"] = uint32(1)
	table["k"] = uint64(1)

	table["strsl"] = []string{"a", "b", "c"}
	table["str"] = "fooooo"

	table["bf"] = false
	table["bt"] = true

	table["bytes"] = []byte("fooooooobbbbbb")

	table["foo"] = float32(2.3)
	table["foo2"] = 2.3

	ret := convHeaders(table)

	require.Equal(t, ret["foo"], []string{"2.30000"})
	require.Equal(t, ret["foo2"], []string{"2.30000"})

	require.Equal(t, ret["a"], []string{"1"})
	require.Equal(t, ret["b"], []string{"1"})
	require.Equal(t, ret["c"], []string{"1"})
	require.Equal(t, ret["d"], []string{"1"})
	require.Equal(t, ret["e"], []string{"1"})

	require.Equal(t, ret["f"], []string{"1"})
	require.Equal(t, ret["j"], []string{"1"})
	require.Equal(t, ret["i"], []string{"1"})
	require.Equal(t, ret["g"], []string{"1"})
	require.Equal(t, ret["k"], []string{"1"})

	require.Equal(t, ret["strsl"], []string{"a", "b", "c"})
	require.Equal(t, ret["str"], []string{"fooooo"})

	require.Equal(t, ret["bf"], []string{"false"})
	require.Equal(t, ret["bt"], []string{"true"})

	require.Equal(t, ret["bytes"], []string{"fooooooobbbbbb"})
}
