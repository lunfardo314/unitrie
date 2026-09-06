package common

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReadBytes32BogusLength guards against a crafted length prefix forcing a
// giant allocation (the FSTATE-4 audit finding): a huge uint32 length followed
// by only a few bytes must return an error after reading the available bytes,
// not pre-allocate up to 4 GiB.
func TestReadBytes32BogusLength(t *testing.T) {
	var buf bytes.Buffer
	var lenPrefix [4]byte
	binary.LittleEndian.PutUint32(lenPrefix[:], 0xFFFFFFFF) // claims ~4 GiB
	buf.Write(lenPrefix[:])
	buf.Write([]byte("only a few bytes")) // far short of the claim

	_, err := ReadBytes32(&buf)
	// The point is that it errors after reading the available bytes rather than
	// pre-allocating ~4 GiB; io.CopyN surfaces the short stream as io.EOF.
	require.Error(t, err)
	require.ErrorIs(t, err, io.EOF)
}

// TestReadBytes32RoundTrip confirms well-formed data still round-trips exactly.
func TestReadBytes32RoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 5000)
	var buf bytes.Buffer
	require.NoError(t, WriteBytes32(&buf, payload))
	got, err := ReadBytes32(&buf)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// TestReadBytes16ShortStream confirms a short stream is reported, not silently
// under-filled with trailing zeros.
func TestReadBytes16ShortStream(t *testing.T) {
	var buf bytes.Buffer
	var lenPrefix [2]byte
	binary.LittleEndian.PutUint16(lenPrefix[:], 100)
	buf.Write(lenPrefix[:])
	buf.Write([]byte("short")) // fewer than 100 bytes

	_, err := ReadBytes16(&buf)
	require.Error(t, err)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
