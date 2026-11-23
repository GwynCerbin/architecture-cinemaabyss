package kafkatopics

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContextReleasePayload(t *testing.T) {
	pool, releasePayload := newPayloadBufferPool(DefaultConfig().DefaultBufferSize)
	payload := &payloadBuffer{data: pool.Get().([]byte), release: releasePayload}
	payload.data = append(payload.data, "payload"...)
	capacity := cap(payload.data)
	delivery := &Context{payload: payload}

	reader := delivery.Reader()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "payload", string(data))
	data, err = io.ReadAll(delivery.Reader())
	require.NoError(t, err)
	require.Equal(t, "payload", string(data), "Reader must rewind the payload")

	delivery.release()
	require.Nil(t, delivery.Bytes())
	require.Nil(t, delivery.payload)
	require.Zero(t, len(payload.data))
	require.Zero(t, payload.offset)
	require.Equal(t, capacity, cap(payload.data), "release must retain the reusable backing array")

	data, err = io.ReadAll(reader)
	require.NoError(t, err)
	require.Empty(t, data)

	delivery.release()
}

func TestContextReaderOffsets(t *testing.T) {
	for _, pooled := range []bool{false, true} {
		name := "data"
		if pooled {
			name = "pooled"
		}

		t.Run(name, func(t *testing.T) {
			delivery := NewContext(context.Background(), []byte("payload"))

			if pooled {
				pool, releasePayload := newPayloadBufferPool(DefaultConfig().DefaultBufferSize)
				delivery.payload = &payloadBuffer{data: pool.Get().([]byte), release: releasePayload}
				delivery.payload.data = append(delivery.payload.data, "payload"...)
			}

			defer delivery.release()

			reader := delivery.Reader()
			n, err := reader.Read(nil)
			require.NoError(t, err)
			require.Zero(t, n)

			prefix := make([]byte, 2)
			n, err = reader.Read(prefix)
			require.NoError(t, err)
			require.Equal(t, 2, n)
			require.Equal(t, "pa", string(prefix))

			var remaining bytes.Buffer

			written, err := io.Copy(&remaining, reader)
			require.NoError(t, err)
			require.EqualValues(t, 5, written)
			require.Equal(t, "yload", remaining.String())
			require.Equal(t, "payload", string(delivery.Bytes()), "reading must not consume the payload buffer")

			n, err = reader.Read(prefix)
			require.ErrorIs(t, err, io.EOF)
			require.Zero(t, n)
			require.Same(t, reader, delivery.Reader(), "rewind reuses the payload reader")

			data, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, "payload", string(data))
		})
	}
}

func TestContextReleaseRetainsOnlyDefaultBufferSize(t *testing.T) {
	for _, size := range []int{512, 1024, 2048} {
		for _, length := range []int{0, size, size + 1, 12 << 10} {
			t.Run(fmt.Sprintf("capacity=%d/payload=%d", size, length), func(t *testing.T) {
				pool, releasePayload := newPayloadBufferPool(size)
				payload := &payloadBuffer{data: pool.Get().([]byte), release: releasePayload}
				require.Equal(t, size, cap(payload.data))
				payload.data = append(payload.data, bytes.Repeat([]byte("x"), length)...)

				delivery := &Context{payload: payload}
				data, err := io.ReadAll(delivery.Reader())
				require.NoError(t, err)
				require.Equal(t, delivery.Bytes(), data)

				delivery.release()

				require.Nil(t, delivery.Bytes())
				require.Nil(t, delivery.payload)

				next := pool.Get().([]byte)
				require.Equal(t, size, cap(next), "grown buffers must not remain in the pool")
				require.Zero(t, len(next))
			})
		}
	}
}

func TestContextReaderEmptyPayload(t *testing.T) {
	delivery := &Context{}
	defer delivery.release()
	require.Nil(t, delivery.Bytes())

	data, err := io.ReadAll(delivery.Reader())
	require.NoError(t, err)
	require.Empty(t, data)
}

func TestContextBytesSharesPayloadWithoutRewinding(t *testing.T) {
	data := []byte("payload")
	delivery := NewContext(context.Background(), data)
	require.Same(t, &data[0], &delivery.Bytes()[0], "wrapping the payload must not copy its bytes")

	reader := delivery.Reader()
	prefix := make([]byte, 2)
	n, err := reader.Read(prefix)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, "pa", string(prefix))
	require.Equal(t, data, delivery.Bytes(), "Bytes returns the whole payload after a partial read")

	remaining, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "yload", string(remaining), "Bytes must not rewind the reader")

	delivery.release()
	require.Nil(t, delivery.Bytes())
}

func TestContextDisposition(t *testing.T) {
	for _, decision := range []Disposition{Ack, Nack, Reject} {
		delivery := &Context{Context: context.Background()}
		require.Equal(t, Pending, delivery.Disposition())
		require.False(t, delivery.IsAcked())
		methods := map[Disposition]func(){Ack: delivery.Ack, Nack: delivery.Nack, Reject: delivery.Reject}
		methods[decision]()
		methods[decision]()
		require.Equal(t, decision, delivery.Disposition())
		require.Equal(t, decision == Ack, delivery.IsAcked())

		for _, method := range methods {
			method()
		}

		require.Equal(t, decision, delivery.Disposition())
	}
}

func TestContextDecisionOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for _, decision := range []Disposition{Ack, Nack, Reject} {
		delivery := &Context{Context: ctx}
		methods := map[Disposition]func(){Ack: delivery.Ack, Nack: delivery.Nack, Reject: delivery.Reject}
		methods[decision]()
		require.Equal(t, decision, delivery.Disposition())
		require.ErrorIs(t, delivery.Err(), context.Canceled)
	}
}
