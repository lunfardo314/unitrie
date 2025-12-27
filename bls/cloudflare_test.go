package bls

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"encoding"
	"fmt"
	"testing"

	"github.com/cloudflare/circl/sign/bls"
	"github.com/stretchr/testify/require"
)

func TestBls(t *testing.T) {
	t.Run("G1/API", testBls[bls.G1])
	t.Run("G2/API", testBls[bls.G2])
	t.Run("G1/Marshal", testMarshalKeys[bls.G1])
	t.Run("G2/Marshal", testMarshalKeys[bls.G2])
	t.Run("G1/Errors", testErrors[bls.G1])
	t.Run("G2/Errors", testErrors[bls.G2])
	t.Run("G1/Aggregation", testAggregation[bls.G1])
	t.Run("G2/Aggregation", testAggregation[bls.G2])
}

func testBls[K bls.KeyGroup](t *testing.T) {
	const testTimes = 1 << 7
	msg := []byte("hello world")
	keyInfo := []byte("KeyInfo for BLS")
	salt := [32]byte{}
	ikm := [32]byte{}
	_, _ = rand.Reader.Read(ikm[:])
	_, _ = rand.Reader.Read(salt[:])

	for i := 0; i < testTimes; i++ {
		_, _ = rand.Reader.Read(ikm[:])

		priv, err := bls.KeyGen[K](ikm[:], salt[:], keyInfo)
		require.NoError(t, err, "failed to keygen")
		signature := bls.Sign(priv, msg)
		pub := priv.Public().(*bls.PublicKey[K])
		require.True(t, bls.Verify(pub, msg, signature), "failed verification", t)
	}
}

func testMarshalKeys[K bls.KeyGroup](t *testing.T) {
	ikm := [32]byte{}
	priv, err := bls.KeyGen[K](ikm[:], nil, nil)
	require.NoError(t, err, "failed to keygen")
	pub := priv.PublicKey()

	auxPriv := new(bls.PrivateKey[K])
	auxPub := new(bls.PublicKey[K])

	t.Run("PrivateKey", func(t *testing.T) {
		testMarshal[K](t, priv, auxPriv)
		require.True(t, priv.Equal(auxPriv), "private keys do not match", t)
	})
	t.Run("PublicKey", func(t *testing.T) {
		testMarshal[K](t, pub, auxPub)
		require.True(t, pub.Equal(auxPub), "public keys do not match", t)
	})
}

func testMarshal[K bls.KeyGroup](
	t *testing.T,
	left, right interface {
		encoding.BinaryMarshaler
		encoding.BinaryUnmarshaler
	},
) {
	want, err := left.MarshalBinary()
	require.NoError(t, err, "failed to marshal")

	err = right.UnmarshalBinary(want)
	require.NoError(t, err, "failed to unmarshal")

	got, err := right.MarshalBinary()
	require.NoError(t, err, "failed to marshal")

	if !bytes.Equal(got, want) {
		t.Errorf("error")
		// test.ReportError(t, got, want)
	}

	err = right.UnmarshalBinary(nil)
	require.Error(t, err, "should fail: empty input")
}

func testErrors[K bls.KeyGroup](t *testing.T) {
	// Short IKM
	_, err := bls.KeyGen[K](nil, nil, nil)
	require.Error(t, err, "should fail: short ikm")

	// Bad Signature size
	ikm := [32]byte{}
	priv, err := bls.KeyGen[K](ikm[:], nil, nil)
	require.NoError(t, err, "failed to keygen")
	pub := priv.PublicKey()
	require.True(t, bls.Verify(pub, nil, nil) == false, "should fail: bad signature", t)

	// Bad public key
	msg := []byte("hello")
	sig := bls.Sign[K](priv, msg)
	pub = new(bls.PublicKey[K])
	require.True(t, pub.Validate() == false, "should fail: bad public key", t)
	require.True(t, bls.Verify(pub, msg, sig) == false, "should fail: bad signature", t)

	// Bad private key
	priv = new(bls.PrivateKey[K])
	require.True(t, priv.Validate() == false, "should fail: bad private key", t)
	require.Panics(t, func() { bls.Sign(priv, msg) })
	//err = test.CheckPanic(func() { bls.Sign(priv, msg) })
	require.NoError(t, err, "sign should panic")

	// Wrong comparisons
	require.True(t, priv.Equal(new(rsa.PrivateKey)) == false, "should fail: bad private key types", t)
	require.True(t, pub.Equal(new(rsa.PublicKey)) == false, "should fail: bad public key types", t)

	// Aggregate nil
	_, err = bls.Aggregate[K](*new(K), nil)
	//test.CheckIsErr(t, err, "should fail: empty signatures")
	require.Error(t, err, "should fail: invalid aggregator")

	// VerifyAggregate nil
	require.True(t, bls.VerifyAggregate([]*bls.PublicKey[K]{}, nil, nil) == false, "should fail: empty keys", t)

	// VerifyAggregate empty signature
	require.True(t, bls.VerifyAggregate([]*bls.PublicKey[K]{pub}, [][]byte{msg}, nil) == false, "should fail: empty signature", t)
}

func testAggregation[K bls.KeyGroup](t *testing.T) {
	const N = 3

	ikm := [32]byte{}
	_, _ = rand.Reader.Read(ikm[:])

	msgs := make([][]byte, N)
	sigs := make([]bls.Signature, N)
	pubKeys := make([]*bls.PublicKey[K], N)

	for i := range sigs {
		priv, err := bls.KeyGen[K](ikm[:], nil, nil)
		require.NoError(t, err, "failed to keygen")
		pubKeys[i] = priv.PublicKey()

		msgs[i] = []byte(fmt.Sprintf("Message number: %v", i))
		sigs[i] = bls.Sign(priv, msgs[i])
	}

	aggSig, err := bls.Aggregate(*new(K), sigs)
	require.NoError(t, err, "failed to aggregate")

	ok := bls.VerifyAggregate(pubKeys, msgs, aggSig)
	require.True(t, ok, "failed to verify aggregated signature", t)
}

func BenchmarkBls(b *testing.B) {
	b.Run("G1", benchmarkBls[bls.G1])
	b.Run("G2", benchmarkBls[bls.G2])
}

func benchmarkBls[K bls.KeyGroup](b *testing.B) {
	msg := []byte("hello world")
	keyInfo := []byte("KeyInfo for BLS")
	salt := [32]byte{}
	ikm := [32]byte{}
	_, _ = rand.Reader.Read(ikm[:])
	_, _ = rand.Reader.Read(salt[:])

	priv, _ := bls.KeyGen[K](ikm[:], salt[:], keyInfo)

	const N = 3
	msgs := make([][]byte, N)
	sigs := make([]bls.Signature, N)
	pubKeys := make([]*bls.PublicKey[K], N)

	for i := range sigs {
		pubKeys[i] = priv.PublicKey()

		msgs[i] = []byte(fmt.Sprintf("Message number: %v", i))
		sigs[i] = bls.Sign(priv, msgs[i])
	}

	b.Run("Keygen", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = rand.Reader.Read(ikm[:])
			_, _ = bls.KeyGen[K](ikm[:], salt[:], keyInfo)
		}
	})

	b.Run("Sign", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = bls.Sign(priv, msg)
		}
	})

	b.Run("Verify", func(b *testing.B) {
		pub := priv.PublicKey()
		signature := bls.Sign(priv, msg)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bls.Verify(pub, msg, signature)
		}
	})

	b.Run("Aggregate3", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = bls.Aggregate(*new(K), sigs)
		}
	})

	b.Run("VerifyAggregate3", func(b *testing.B) {
		aggSig, _ := bls.Aggregate(*new(K), sigs)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = bls.VerifyAggregate(pubKeys, msgs, aggSig)
		}
	})
}
