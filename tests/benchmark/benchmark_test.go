// Package benchmark provides performance benchmarks for RapidoDB components.
package benchmark

import (
	"bytes"
	"testing"

	"github.com/rapidodb/rapidodb/internal/encoding"
	"github.com/rapidodb/rapidodb/pkg/types"
)

// Entry encoding benchmarks

func BenchmarkEncodeEntrySmall(b *testing.B) {
	entry := &types.Entry{
		Key:    []byte("key"),
		Value:  []byte("value"),
		Type:   types.EntryTypePut,
		SeqNum: 1,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoding.EncodeEntry(entry)
	}
}

func BenchmarkEncodeEntryMedium(b *testing.B) {
	entry := &types.Entry{
		Key:    bytes.Repeat([]byte("k"), 16),
		Value:  bytes.Repeat([]byte("v"), 100),
		Type:   types.EntryTypePut,
		SeqNum: 12345,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoding.EncodeEntry(entry)
	}
}

func BenchmarkEncodeEntryLarge(b *testing.B) {
	entry := &types.Entry{
		Key:    bytes.Repeat([]byte("k"), 64),
		Value:  bytes.Repeat([]byte("v"), 4096),
		Type:   types.EntryTypePut,
		SeqNum: 12345,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoding.EncodeEntry(entry)
	}
}

func BenchmarkDecodeEntrySmall(b *testing.B) {
	entry := &types.Entry{
		Key:    []byte("key"),
		Value:  []byte("value"),
		Type:   types.EntryTypePut,
		SeqNum: 1,
	}
	encoded := encoding.EncodeEntry(entry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoding.DecodeEntry(encoded)
	}
}

func BenchmarkDecodeEntryMedium(b *testing.B) {
	entry := &types.Entry{
		Key:    bytes.Repeat([]byte("k"), 16),
		Value:  bytes.Repeat([]byte("v"), 100),
		Type:   types.EntryTypePut,
		SeqNum: 12345,
	}
	encoded := encoding.EncodeEntry(entry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoding.DecodeEntry(encoded)
	}
}

func BenchmarkDecodeEntryLarge(b *testing.B) {
	entry := &types.Entry{
		Key:    bytes.Repeat([]byte("k"), 64),
		Value:  bytes.Repeat([]byte("v"), 4096),
		Type:   types.EntryTypePut,
		SeqNum: 12345,
	}
	encoded := encoding.EncodeEntry(entry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoding.DecodeEntry(encoded)
	}
}

// Checksum benchmarks

func BenchmarkChecksum4KB(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoding.Checksum(data)
	}
}

func BenchmarkChecksum64KB(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 64*1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoding.Checksum(data)
	}
}

// InternalKey benchmarks

func BenchmarkInternalKeyCompare(b *testing.B) {
	k1 := types.NewInternalKey([]byte("user-key-12345"), 100, types.EntryTypePut)
	k2 := types.NewInternalKey([]byte("user-key-12345"), 50, types.EntryTypePut)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k1.Compare(k2)
	}
}

func BenchmarkInternalKeyCompareDifferentKeys(b *testing.B) {
	k1 := types.NewInternalKey([]byte("user-key-aaaaa"), 100, types.EntryTypePut)
	k2 := types.NewInternalKey([]byte("user-key-zzzzz"), 100, types.EntryTypePut)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k1.Compare(k2)
	}
}

// KeyRange benchmarks

func BenchmarkKeyRangeContains(b *testing.B) {
	r := types.NewKeyRange([]byte("aaaa"), []byte("zzzz"))
	key := []byte("mmmm")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Contains(key)
	}
}

func BenchmarkKeyRangeOverlaps(b *testing.B) {
	r1 := types.NewKeyRange([]byte("aaaa"), []byte("mmmm"))
	r2 := types.NewKeyRange([]byte("kkkk"), []byte("zzzz"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r1.Overlaps(r2)
	}
}

// Entry operations benchmarks

func BenchmarkEntryClone(b *testing.B) {
	entry := &types.Entry{
		Key:    bytes.Repeat([]byte("k"), 16),
		Value:  bytes.Repeat([]byte("v"), 100),
		Type:   types.EntryTypePut,
		SeqNum: 12345,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.Clone()
	}
}

func BenchmarkEntrySize(b *testing.B) {
	entry := &types.Entry{
		Key:    bytes.Repeat([]byte("k"), 16),
		Value:  bytes.Repeat([]byte("v"), 100),
		Type:   types.EntryTypePut,
		SeqNum: 12345,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = entry.Size()
	}
}
