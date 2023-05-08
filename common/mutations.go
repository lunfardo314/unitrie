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
		if _, wasSet := m.set[ks]; wasSet {
			m.set[ks] = nil // storing information it was set before delete
		}
		m.del[ks] = struct{}{}
	}
}

// Iterate is special iteration for mutations. It first iterates SET mutations, then DEL mutations
// On SET mutation, k, v != nil and wasSet = true
// On DEL mutation, k != nil, v == nil. wasSet is true if value was set before delete, otherwise false
// The wasSet allows control deletion of keys which must exist in the original state, e.g. UTXO ledger state
func (m *Mutations) Iterate(fun func(k []byte, v []byte, wasSet bool) bool) {
	for k, v := range m.set {
		if len(v) > 0 {
			fun([]byte(k), v, true)
		}
	}
	for k := range m.del {
		v, wasSet := m.set[k]
		Assert(len(v) == 0, "len(v)==0")
		fun([]byte(k), nil, wasSet)
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
	m.Iterate(func(k []byte, v []byte, wasSet bool) bool {
		if len(v) > 0 {
			ret = append(ret, fmt.Sprintf("SET '%s' = '%s'", string(k), string(v)))
		} else {
			ret = append(ret, fmt.Sprintf("DEL '%s' (wasSet = %v)", string(k), wasSet))
		}
		return true
	})
	return strings.Join(ret, "\n")
}
