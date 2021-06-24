package zed

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/brimdata/zed/zcode"
)

type TypeMap struct {
	id      int
	KeyType Type
	ValType Type
}

func NewTypeMap(id int, keyType, valType Type) *TypeMap {
	return &TypeMap{id, keyType, valType}
}

func (t *TypeMap) ID() int {
	return t.id
}

func (t *TypeMap) String() string {
	return fmt.Sprintf("|{%s,%s|}", t.KeyType, t.ValType)
}

func (t *TypeMap) Decode(zv zcode.Bytes) (Value, Value, error) {
	if zv == nil {
		return Value{}, Value{}, nil
	}
	it := zv.Iter()
	key, container, err := it.Next()
	if err != nil {
		return Value{}, Value{}, err
	}
	if container != IsContainerType(t.KeyType) {
		return Value{}, Value{}, ErrMismatch
	}
	var val zcode.Bytes
	val, container, err = it.Next()
	if err != nil {
		return Value{}, Value{}, err
	}
	if container != IsContainerType(t.ValType) {
		return Value{}, Value{}, ErrMismatch
	}
	return Value{t.KeyType, key}, Value{t.ValType, val}, nil
}

func (t *TypeMap) Marshal(zv zcode.Bytes) (interface{}, error) {
	// start out with zero-length container so we get "[]" instead of nil
	vals := []Value{}
	it := zv.Iter()
	for !it.Done() {
		val, _, err := it.Next()
		if err != nil {
			return nil, err
		}
		vals = append(vals, Value{t.KeyType, val})
		val, _, err = it.Next()
		if err != nil {
			return nil, err
		}
		vals = append(vals, Value{t.ValType, val})
	}
	return vals, nil
}

type keyval struct {
	key zcode.Bytes
	val zcode.Bytes
}

// NormalizeMap interprets zv as a map body and returns an equivalent map body
// that is normalized according to the ZNG specification (i.e., the tag-counted
// value of each entry's key is lexicographically greater than that of the
// preceding entry).
func NormalizeMap(zv zcode.Bytes) zcode.Bytes {
	elements := make([]keyval, 0, 8)
	for it := zv.Iter(); !it.Done(); {
		key, _, err := it.NextTagAndBody()
		if err != nil {
			panic(err)
		}
		val, _, err := it.NextTagAndBody()
		if err != nil {
			panic(err)
		}
		elements = append(elements, keyval{key, val})
	}
	if len(elements) < 2 {
		return zv
	}
	sort.Slice(elements, func(i, j int) bool {
		return bytes.Compare(elements[i].key, elements[j].key) == -1
	})
	norm := make(zcode.Bytes, 0, len(zv))
	norm = append(norm, elements[0].key...)
	norm = append(norm, elements[0].val...)
	for i := 1; i < len(elements); i++ {
		// Skip duplicates.
		if !bytes.Equal(elements[i].key, elements[i-1].key) {
			norm = append(norm, elements[i].key...)
			norm = append(norm, elements[i].val...)
		}
	}
	return norm
}

func (t *TypeMap) Format(zv zcode.Bytes) string {
	var b strings.Builder
	it := zv.Iter()
	b.WriteString("|{")
	sep := ""
	for !it.Done() {
		val, _, err := it.Next()
		if err != nil {
			return badZng(err, t, zv)
		}
		b.WriteString(sep)
		b.WriteByte('{')
		b.WriteString(t.KeyType.Format(val))
		b.WriteByte(',')
		val, _, err = it.Next()
		if err != nil {
			return badZng(err, t, zv)
		}
		b.WriteString(t.ValType.Format(val))
		b.WriteByte('}')
		b.WriteString(sep)
		sep = ","
	}
	b.WriteString("}|")
	return b.String()
}
