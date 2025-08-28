package atomic

import (
	"encoding/json"
	"math"
	"sync/atomic"
)

type TypedValue[T any] struct {
	value atomic.Pointer[T]
}

func (t *TypedValue[T]) Load() (v T) {
	v, _ = t.LoadOk()
	return
}

func (t *TypedValue[T]) LoadOk() (v T, ok bool) {
	value := t.value.Load()
	if value == nil {
		return
	}
	return *value, true
}

func (t *TypedValue[T]) Store(value T) {
	t.value.Store(&value)
}

func (t *TypedValue[T]) Swap(new T) (v T) {
	old := t.value.Swap(&new)
	if old == nil {
		return
	}
	return *old
}

func (t *TypedValue[T]) CompareAndSwap(old, new T) bool {
	for {
		currentP := t.value.Load()
		var currentValue T
		if currentP != nil {
			currentValue = *currentP
		}
		// Compare old and current via runtime equality check.
		if any(currentValue) != any(old) {
			return false
		}
		if t.value.CompareAndSwap(currentP, &new) {
			return true
		}
	}
}

func (t *TypedValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Load())
}

func (t *TypedValue[T]) UnmarshalJSON(b []byte) error {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	t.Store(v)
	return nil
}

func (t *TypedValue[T]) MarshalYAML() (any, error) {
	return t.Load(), nil
}

func (t *TypedValue[T]) UnmarshalYAML(unmarshal func(any) error) error {
	var v T
	if err := unmarshal(&v); err != nil {
		return err
	}
	t.Store(v)
	return nil
}

func NewTypedValue[T any](t T) (v TypedValue[T]) {
	v.Store(t)
	return
}

// TypedValue[map[K]V]
func (t *TypedValue[T]) Update(f func(old T) (new T)) {
	var zero T
	switch any(zero).(type) {
	case map[string]float64:
		old := t.Load()
		new := f(old)
		t.Store(new)
		return
	}
	for {
		old := t.Load()
		new := f(old)
		if t.CompareAndSwap(old, new) {
			return
		}
	}
}

func CloneMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return nil
	}
	newMap := make(map[K]V, len(m))
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}

// atomic.Float64
type Float64 struct {
	_     uint64
	value uint64
}

func (f *Float64) Store(val float64) {
	atomic.StoreUint64(&f.value, math.Float64bits(val))
}

func (f *Float64) Load() float64 {
	return math.Float64frombits(atomic.LoadUint64(&f.value))
}

func (f *Float64) Add(delta float64) float64 {
	for {
		oldBits := atomic.LoadUint64(&f.value)
		old := math.Float64frombits(oldBits)
		new := old + delta
		newBits := math.Float64bits(new)
		if atomic.CompareAndSwapUint64(&f.value, oldBits, newBits) {
			return new
		}
	}
}

func (f *Float64) Swap(new float64) float64 {
	for {
		oldBits := atomic.LoadUint64(&f.value)
		newBits := math.Float64bits(new)
		if atomic.CompareAndSwapUint64(&f.value, oldBits, newBits) {
			return math.Float64frombits(oldBits)
		}
	}
}

func (f *Float64) MarshalJSON() ([]byte, error) {
	return json.Marshal(f.Load())
}

func (f *Float64) UnmarshalJSON(b []byte) error {
	var v float64
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	f.Store(v)
	return nil
}
