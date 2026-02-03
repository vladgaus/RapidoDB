// Package benchmark provides performance benchmarks for RapidoDB components.
package benchmark

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/rapidodb/rapidodb/internal/encoding"
	"github.com/rapidodb/rapidodb/pkg/memtable"
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

// MemTable benchmarks

func BenchmarkMemTablePutSmall(b *testing.B) {
	mt := memtable.NewMemTable(1, 1024*1024*1024)

	keys := make([][]byte, b.N)
	values := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i))
		values[i] = []byte("value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.Put(keys[i], values[i], uint64(i+1))
	}
}

func BenchmarkMemTablePutLarge(b *testing.B) {
	mt := memtable.NewMemTable(1, 1024*1024*1024)
	largeValue := bytes.Repeat([]byte("v"), 1024)

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.Put(keys[i], largeValue, uint64(i+1))
	}
}

func BenchmarkMemTableGetHit(b *testing.B) {
	mt := memtable.NewMemTable(1, 1024*1024*1024)
	n := 100000

	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i))
		mt.Put(keys[i], []byte("value"), uint64(i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.Get(keys[i%n], ^uint64(0))
	}
}

func BenchmarkMemTableGetMiss(b *testing.B) {
	mt := memtable.NewMemTable(1, 1024*1024*1024)
	n := 100000

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		mt.Put(key, []byte("value"), uint64(i+1))
	}

	missKeys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		missKeys[i] = []byte(fmt.Sprintf("miss%08d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mt.Get(missKeys[i%1000], ^uint64(0))
	}
}

func BenchmarkMemTableIterateAll(b *testing.B) {
	mt := memtable.NewMemTable(1, 1024*1024*1024)
	n := 10000

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		mt.Put(key, []byte("value"), uint64(i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := mt.NewIterator()
		iter.SeekToFirst()
		count := 0
		for iter.Valid() {
			_ = iter.Key()
			iter.Next()
			count++
		}
		iter.Close()
	}
}

func BenchmarkMemTableSeek(b *testing.B) {
	mt := memtable.NewMemTable(1, 1024*1024*1024)
	n := 100000

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		mt.Put(key, []byte("value"), uint64(i+1))
	}

	seekKeys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		seekKeys[i] = []byte(fmt.Sprintf("key%08d", i*100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := mt.NewIterator()
		iter.Seek(seekKeys[i%1000])
		iter.Close()
	}
}

