package common

import (
	"fmt"
	"strings"
)

type Mutations struct {
	set                 map[string][]byte
	del                 map[string]struct{}
	mustNoDoubleBooking func(error) // is called on double setting and double deleting
}

func NewMutations(doubleBookingCallback ...func(error)) *Mutations {
	ret := &Mutations{
		set: make(map[string][]byte),
		del: make(map[string]struct{}),
	}
	if len(doubleBookingCallback) > 0 {
		ret.mustNoDoubleBooking = doubleBookingCallback[0]
	}
	return ret
}

func NewMutationsMustNoDoubleBooking() *Mutations {
	return NewMutations(func(err error) {
		panic(err)
	})
}

func (m *Mutations) Set(k, v []byte) {
	ks := string(k)
	if m.mustNoDoubleBooking != nil {
		if len(v) > 0 {
			// set
			if _, already := m.set[ks]; already {
				m.mustNoDoubleBooking(fmt.Errorf("repetitive SET mutation. The key '%s' was already set", ks))
			} else if _, already = m.del[ks]; already {
				m.mustNoDoubleBooking(fmt.Errorf("repetitive SET mutation. The key '%s' was already deleted", ks))
			}
		} else {
			// delete
			if _, already := m.del[ks]; already {
				m.mustNoDoubleBooking(fmt.Errorf("repetitive DEL mutation. The key '%s' was already deleted", ks))
			}
		}
	}
	if len(v) > 0 {
		delete(m.del, ks)
		m.set[ks] = v
	} else {
		delete(m.set, ks)
		m.del[ks] = struct{}{}
	}
}

func (m *Mutations) Iterate(fun func(k []byte, v []byte) bool) {
	for k, v := range m.set {
		fun([]byte(k), v)
	}
	for k := range m.del {
		fun([]byte(k), nil)
	}
}

func (m *Mutations) WriteTo(w KVWriter) {
	for k, v := range m.set {
		w.Set([]byte(k), v)
	}
	for k := range m.del {
		w.Set([]byte(k), nil)
	}
}

func (m *Mutations) LenSet() int {
	return len(m.set)
}

func (m *Mutations) LenDel() int {
	return len(m.del)
}

func (m *Mutations) String() string {
	ret := make([]string, 0)
	m.Iterate(func(k []byte, v []byte) bool {
		if len(v) > 0 {
			ret = append(ret, fmt.Sprintf("SET '%s' = '%s'", string(k), string(v)))
		} else {
			ret = append(ret, fmt.Sprintf("DEL '%s'", string(k)))
		}
		return true
	})
	return strings.Join(ret, "\n")
}