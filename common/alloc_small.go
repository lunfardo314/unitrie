package common

import "sync"

const (
	keepInPoolMinSize = 8
	keepInPoolMaxSize = 256
)

var keyPool [keepInPoolMaxSize + 1]sync.Pool

func AllocSmallBuf(size int) (ret []byte) {
	if size < keepInPoolMinSize || size > keepInPoolMaxSize {
		ret = make([]byte, 0, size)
	} else {
		if r := keyPool[size].Get(); r != nil {
			ret = r.([]byte)[:0]
		} else {
			ret = make([]byte, 0, size)
		}
	}
	return
}

func DisposeSmallBuf(b []byte) {
	if len(b) < keepInPoolMinSize || len(b) > keepInPoolMaxSize {
		return
	}
	for i := range b {
		b[i] = 0
	}
	keyPool[len(b)].Put(b)
}
