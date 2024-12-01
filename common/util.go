package common

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
)

// CheckNils returns (conclusive comparison result, true) if at least one is nil
// return (false, false) if both are not nil and can both be safely dereferenced
func CheckNils(i1, i2 interface{}) (bool, bool) {
	// TODO better suggestion? The problem: type(nil) != nil
	i1Nil := i1 == nil || (reflect.ValueOf(i1).Kind() == reflect.Ptr && reflect.ValueOf(i1).IsNil())
	i2Nil := i2 == nil || (reflect.ValueOf(i2).Kind() == reflect.Ptr && reflect.ValueOf(i2).IsNil())
	if i1Nil && i2Nil {
		return true, true
	}
	if i1Nil || i2Nil {
		return false, true
	}
	return false, false
}

// MustBytes most common way of serialization
func MustBytes(o interface{ Write(w io.Writer) error }) []byte {
	var buf bytes.Buffer
	if err := o.Write(&buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// byteCounter simple byte counter as io.Writer
type byteCounter int

func (b *byteCounter) Write(p []byte) (n int, err error) {
	*b = byteCounter(int(*b) + len(p))
	return 0, nil
}

// Size calculates byte size of the serializable object
func Size(o interface{ Write(w io.Writer) error }) (int, error) {
	var ret byteCounter
	if err := o.Write(&ret); err != nil {
		return 0, err
	}
	return int(ret), nil
}

// MustSize calculates byte size of the serializable object
func MustSize(o interface{ Write(w io.Writer) error }) int {
	ret, err := Size(o)
	if err != nil {
		panic(err)
	}
	return ret
}

// Concat concatenates bytes of byte-able objects
func Concat(par ...interface{}) []byte {
	var buf bytes.Buffer
	for _, p := range par {
		switch p := p.(type) {
		case []byte:
			buf.Write(p)
		case byte:
			buf.WriteByte(p)
		case string:
			buf.Write([]byte(p))
		case interface{ Bytes() []byte }:
			buf.Write(p.Bytes())
		case int:
			if p < 0 || p > 255 {
				panic("Concat: not a 1 byte integer value")
			}
			buf.WriteByte(byte(p))
		default:
			Assertf(false, "Concat: unsupported type %T", p)
		}
	}
	return buf.Bytes()
}

// concatBytes allocates exact size array
func concatBytes(data ...[]byte) []byte {
	size := 0
	for _, d := range data {
		size += len(d)
	}
	ret := AllocSmallBuf(size)
	for _, d := range data {
		ret = append(ret, d...)
	}
	return ret
}

// UseConcatBytes optimized for temporary buf
func UseConcatBytes(fun func(cat []byte), data ...[]byte) {
	cat := concatBytes(data...)
	fun(cat)
	DisposeSmallBuf(cat)
}

// writeKV serializes key/value pair into the io.Writer. 2 and 4 little endian bytes for respectively key length and value length
func writeKV(w io.Writer, k, v []byte) (int, error) {
	if err := WriteBytes16(w, k); err != nil {
		return 0, err
	}
	if err := WriteBytes32(w, v); err != nil {
		return len(k) + 2, err
	}
	return len(k) + len(v) + 6, nil
}

// readKV deserializes key/value pair from io.Reader. Returns key/value pair and an error flag if not enough data
func readKV(r io.Reader) ([]byte, []byte, bool) {
	k, err := ReadBytes16(r)
	if errors.Is(err, io.EOF) {
		return nil, nil, true
	}
	v, err := ReadBytes32(r)
	if err != nil {
		panic(err)
	}
	return k, v, false
}

// ---------------------------------------------------------------------------
// r/w utility functions
// TODO rewrite with generics when switch to Go 1.18

func ReadBytes8(r io.Reader) ([]byte, error) {
	length, err := ReadByte(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return []byte{}, nil
	}
	ret := make([]byte, length)
	_, err = r.Read(ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func WriteBytes8(w io.Writer, data []byte) error {
	if len(data) > 256 {
		panic(fmt.Sprintf("WriteBytes8: too long data (%v)", len(data)))
	}
	err := WriteByte(w, byte(len(data)))
	if err != nil {
		return err
	}
	if len(data) != 0 {
		_, err = w.Write(data)
	}
	return err
}

func ReadBytes16(r io.Reader) ([]byte, error) {
	var length uint16
	err := ReadUint16(r, &length)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return []byte{}, nil
	}
	ret := make([]byte, length)
	_, err = r.Read(ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func WriteBytes16(w io.Writer, data []byte) error {
	if len(data) > math.MaxUint16 {
		panic(fmt.Sprintf("WriteBytes16: too long data (%v)", len(data)))
	}
	err := WriteUint16(w, uint16(len(data)))
	if err != nil {
		return err
	}
	if len(data) != 0 {
		_, err = w.Write(data)
	}
	return err
}

func ReadUint16(r io.Reader, pval *uint16) error {
	var tmp2 [2]byte
	_, err := r.Read(tmp2[:])
	if err != nil {
		return err
	}
	*pval = binary.LittleEndian.Uint16(tmp2[:])
	return nil
}

func WriteUint16(w io.Writer, val uint16) error {
	_, err := w.Write(Uint16To2Bytes(val))
	return err
}

func Uint16To2Bytes(val uint16) []byte {
	var tmp2 [2]byte
	binary.LittleEndian.PutUint16(tmp2[:], val)
	return tmp2[:]
}

func Uint16From2Bytes(b []byte) (uint16, error) {
	if len(b) != 2 {
		return 0, errors.New("len(b) != 2")
	}
	return binary.LittleEndian.Uint16(b), nil
}

func ReadByte(r io.Reader) (byte, error) {
	var b [1]byte
	_, err := r.Read(b[:])
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func WriteByte(w io.Writer, val byte) error {
	b := []byte{val}
	_, err := w.Write(b)
	return err
}

func ReadBytes32(r io.Reader) ([]byte, error) {
	var length uint32
	err := ReadUint32(r, &length)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return []byte{}, nil
	}
	ret := make([]byte, length)
	_, err = r.Read(ret)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func WriteBytes32(w io.Writer, data []byte) error {
	if len(data) > math.MaxUint32 {
		panic(fmt.Sprintf("WriteBytes32: too long data (%v)", len(data)))
	}
	err := WriteUint32(w, uint32(len(data)))
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func Uint32To4Bytes(val uint32) []byte {
	var tmp4 [4]byte
	binary.LittleEndian.PutUint32(tmp4[:], val)
	return tmp4[:]
}

func Uint32From4Bytes(b []byte) (uint32, error) {
	if len(b) != 4 {
		return 0, errors.New("len(b) != 4")
	}
	return binary.LittleEndian.Uint32(b), nil
}

func MustUint32From4Bytes(b []byte) uint32 {
	ret, err := Uint32From4Bytes(b)
	if err != nil {
		panic(err)
	}
	return ret
}

func ReadUint32(r io.Reader, pval *uint32) error {
	var tmp4 [4]byte
	_, err := r.Read(tmp4[:])
	if err != nil {
		return err
	}
	*pval = MustUint32From4Bytes(tmp4[:])
	return nil
}

func WriteUint32(w io.Writer, val uint32) error {
	_, err := w.Write(Uint32To4Bytes(val))
	return err
}

func Blake2b160(data []byte) (ret [20]byte) {
	hash, _ := blake2b.New(20, nil)
	if _, err := hash.Write(data); err != nil {
		panic(err)
	}
	copy(ret[:], hash.Sum(nil))
	return
}

func IsNil(p interface{}) bool {
	return p == nil || (reflect.ValueOf(p).Kind() == reflect.Ptr && reflect.ValueOf(p).IsNil())
}

func CatchPanicOrError(f func() error) error {
	var err error
	func() {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			var ok bool
			if err, ok = r.(error); !ok {
				err = fmt.Errorf("%v", r)
			}
		}()
		err = f()
	}()
	return err
}

func RequireErrorWith(t *testing.T, err error, fragments ...string) {
	require.Error(t, err)
	for _, f := range fragments {
		require.Contains(t, err.Error(), f)
	}
}

func RequirePanicOrErrorWith(t *testing.T, f func() error, fragments ...string) {
	RequireErrorWith(t, CatchPanicOrError(f), fragments...)
}

// Assertf with optionally deferred evaluation of arguments
func Assertf(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Errorf("assertion failed:: "+format, EvalLazyArgs(args...)...))
	}
}

func AssertNoError(err error, prefix ...string) {
	pref := "error: "
	if len(prefix) > 0 {
		pref = strings.Join(prefix, " ") + ": "
	}
	Assertf(err == nil, pref+"%w", err)
}

func EvalLazyArgs(args ...any) []any {
	ret := make([]any, len(args))
	for i, arg := range args {
		switch funArg := arg.(type) {
		case func() string:
			ret[i] = funArg()
		case func() bool:
			ret[i] = funArg()
		case func() int:
			ret[i] = funArg()
		case func() byte:
			ret[i] = funArg()
		case func() uint:
			ret[i] = funArg()
		case func() uint16:
			ret[i] = funArg()
		case func() uint32:
			ret[i] = funArg()
		case func() uint64:
			ret[i] = funArg()
		case func() int16:
			ret[i] = funArg()
		case func() int32:
			ret[i] = funArg()
		case func() int64:
			ret[i] = funArg()
		case func() any:
			ret[i] = funArg()
		default:
			ret[i] = arg
		}
	}
	return ret
}
