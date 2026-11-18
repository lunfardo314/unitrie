package common

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// Decoders read data from untrusted sources (proofs from a remote node, snapshot
// files), so malformed input must return an error, never panic, and a stream cut
// inside a record must not pass for the clean end of the stream.

// A header claiming padding with no packed byte behind it used to index out of
// range in decode16 and fail an assertion in decode2.
func TestDecodeMalformedNoPanic(t *testing.T) {
	for _, arity := range AllPathArity {
		for _, data := range [][]byte{{0x01}, {0x02}, {0x07}, {0x08}, {0xff}, {0x01, 0x01}, {0x07, 0xff}} {
			require.NotPanics(t, func() { _, _ = DecodeToUnpackedBytes(data, arity) }, "arity %s data %x", arity, data)
		}
	}
	_, err := decode16([]byte{0x01})
	require.ErrorIs(t, err, ErrWrongFormat)
	_, err = decode2([]byte{0x03})
	require.ErrorIs(t, err, ErrWrongFormat)
}

// Every truncation returns an error. A cut between two fields may read as io.EOF:
// a fixed-size reader meeting the end before its first byte reports the clean end,
// which is how a reader looping over records stops. A cut inside a field, or after
// a length prefix announcing data, is io.ErrUnexpectedEOF.
func TestReadersTruncated(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteByte(&buf, 7))
	require.NoError(t, WriteUint16(&buf, 0x1234))
	require.NoError(t, WriteUint32(&buf, 0x12345678))
	require.NoError(t, WriteBytes8(&buf, []byte("eight")))
	require.NoError(t, WriteBytes16(&buf, []byte("sixteen")))
	require.NoError(t, WriteBytes32(&buf, []byte("thirty-two")))
	full := buf.Bytes()

	readAll := func(r io.Reader) error {
		if _, err := ReadByte(r); err != nil {
			return err
		}
		var v16 uint16
		if err := ReadUint16(r, &v16); err != nil {
			return err
		}
		var v32 uint32
		if err := ReadUint32(r, &v32); err != nil {
			return err
		}
		for _, read := range []func(io.Reader) ([]byte, error){ReadBytes8, ReadBytes16, ReadBytes32} {
			if _, err := read(r); err != nil {
				return err
			}
		}
		return nil
	}
	require.NoError(t, readAll(bytes.NewReader(full)))
	require.ErrorIs(t, readAll(bytes.NewReader(nil)), io.EOF)
	// offsets between the fields: after the byte, the uint16, the uint32, the 8-bit and the 16-bit records
	fieldEnds := map[int]bool{1: true, 3: true, 7: true, 13: true, 22: true}
	for i := 1; i < len(full); i++ {
		// a reader handing out one byte per Read: a single r.Read would under-fill
		err := readAll(&oneByteReader{bytes.NewReader(full[:i])})
		require.Error(t, err, "truncated at %d", i)
		if !fieldEnds[i] {
			require.ErrorIs(t, err, io.ErrUnexpectedEOF, "truncated at %d", i)
		}
	}
	// a stream delivering one byte per Read is not truncated
	require.NoError(t, readAll(&oneByteReader{bytes.NewReader(full)}))
}

type oneByteReader struct{ r io.Reader }

func (o *oneByteReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return o.r.Read(p[:1])
}

// Writers refuse data longer than their length prefix with an error instead of panicking.
func TestWritersTooLong(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, WriteBytes8(&buf, make([]byte, 255)))
	require.Error(t, WriteBytes8(&buf, make([]byte, 256)))
	require.NoError(t, WriteBytes16(&buf, make([]byte, 65535)))
	require.Error(t, WriteBytes16(&buf, make([]byte, 65536)))
}

// A key/value stream cut anywhere but between records is an error: only a stream
// ending exactly after a complete record ends the iteration cleanly.
func TestBinaryStreamTruncated(t *testing.T) {
	var buf bytes.Buffer
	w := NewBinaryStreamWriter(&buf)
	require.NoError(t, w.Write([]byte("key1"), []byte("value1")))
	require.NoError(t, w.Write([]byte("key2"), []byte("value2")))
	full := buf.Bytes()
	recordLen := 2 + 4 + 4 + 6

	for i := 0; i <= len(full); i++ {
		n := 0
		err := NewBinaryStreamIterator(bytes.NewReader(full[:i])).Iterate(func(k, v []byte) bool { n++; return true })
		if i%recordLen == 0 {
			require.NoError(t, err, "cut at %d", i)
			require.Equal(t, i/recordLen, n)
		} else {
			require.ErrorIs(t, err, io.ErrUnexpectedEOF, "cut at %d", i)
		}
	}
}
