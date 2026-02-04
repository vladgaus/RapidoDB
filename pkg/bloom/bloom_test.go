package bloom

import (
	"fmt"
	"testing"
)

func TestFilterBasic(t *testing.T) {
	f := New(100, 10)

	// Add some keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		f.Add([]byte(k))
	}

	// All added keys should be found
	for _, k := range keys {
		if !f.MayContain([]byte(k)) {
			t.Errorf("key %q should be found", k)
		}
	}

	// Keys not added should (usually) not be found
	// Note: false positives are possible, so we just check a few
	notAdded := []string{"fig", "grape", "honeydew"}
	found := 0
	for _, k := range notAdded {
		if f.MayContain([]byte(k)) {
			found++
		}
	}
	// With 10 bits/key, false positive rate is ~1%, so finding all 3 would be suspicious
	if found == len(notAdded) {
		t.Logf("warning: all non-added keys found (possible but unlikely)")
	}
}

func TestFilterEmpty(t *testing.T) {
	f := New(10, 10)

	// Empty filter should not contain anything
	if f.MayContain([]byte("test")) {
		t.Error("empty filter should not contain any key")
	}
}

func TestFilterEncodeDecode(t *testing.T) {
	f := New(100, 10)

	// Add keys
	for i := 0; i < 50; i++ {
		f.Add([]byte(fmt.Sprintf("key%d", i)))
	}

	// Encode
	data := f.Encode()
	if len(data) == 0 {
		t.Fatal("encoded data should not be empty")
	}

	// Decode
	f2, err := Decode(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify same properties
	if f2.Size() != f.Size() {
		t.Errorf("size mismatch: got %d, want %d", f2.Size(), f.Size())
	}
	if f2.NumHashes() != f.NumHashes() {
		t.Errorf("num hashes mismatch: got %d, want %d", f2.NumHashes(), f.NumHashes())
	}

	// Verify all keys are found
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if !f2.MayContain(key) {
			t.Errorf("key %s should be found after decode", key)
		}
	}
}

func TestFilterFalsePositiveRate(t *testing.T) {
	numKeys := 10000
	bitsPerKey := 10 // Should give ~1% FP rate

	f := New(numKeys, bitsPerKey)

	// Add keys
	for i := 0; i < numKeys; i++ {
		f.Add([]byte(fmt.Sprintf("key%08d", i)))
	}

	// Test false positive rate with keys that were NOT added
	falsePositives := 0
	numTests := 100000

	for i := 0; i < numTests; i++ {
		key := []byte(fmt.Sprintf("notakey%08d", i))
		if f.MayContain(key) {
			falsePositives++
		}
	}

	fpRate := float64(falsePositives) / float64(numTests)
	t.Logf("False positive rate: %.2f%% (%d/%d)", fpRate*100, falsePositives, numTests)

	// With 10 bits/key, we expect ~1% FP rate
	// Allow up to 3% to account for variance
	if fpRate > 0.03 {
		t.Errorf("false positive rate too high: %.2f%% (expected ~1%%)", fpRate*100)
	}
}

func TestFilterNoFalseNegatives(t *testing.T) {
	numKeys := 1000
	f := New(numKeys, 10)

	// Add keys
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i))
		f.Add(keys[i])
	}

	// Verify NO false negatives
	for i, key := range keys {
		if !f.MayContain(key) {
			t.Errorf("false negative for key %d: %s", i, key)
		}
	}
}

func TestFilterDifferentSizes(t *testing.T) {
	tests := []struct {
		numKeys    int
		bitsPerKey int
	}{
		{10, 10},
		{100, 10},
		{1000, 10},
		{100, 5},
		{100, 15},
		{100, 20},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("n=%d,b=%d", tt.numKeys, tt.bitsPerKey), func(t *testing.T) {
			f := New(tt.numKeys, tt.bitsPerKey)

			// Add keys
			for i := 0; i < tt.numKeys; i++ {
				f.Add([]byte(fmt.Sprintf("key%d", i)))
			}

			// Verify all keys found
			for i := 0; i < tt.numKeys; i++ {
				if !f.MayContain([]byte(fmt.Sprintf("key%d", i))) {
					t.Errorf("key%d not found", i)
				}
			}
		})
	}
}

func TestFilterMinimumSize(t *testing.T) {
	// Very small filter should still work
	f := New(1, 1)
	f.Add([]byte("test"))

	if !f.MayContain([]byte("test")) {
		t.Error("small filter should still work")
	}

	// Size should be at least 8 bytes (64 bits minimum)
	if f.Size() < 8 {
		t.Errorf("minimum size should be 8 bytes, got %d", f.Size())
	}
}

func TestDecodeInvalid(t *testing.T) {
	// Empty data
	_, err := Decode([]byte{})
	if err != ErrInvalidFilter {
		t.Errorf("expected ErrInvalidFilter for empty data, got %v", err)
	}

	// Single byte
	_, err = Decode([]byte{0x00})
	if err != ErrInvalidFilter {
		t.Errorf("expected ErrInvalidFilter for single byte, got %v", err)
	}
}

func TestNewWithData(t *testing.T) {
	// Create filter and populate
	f1 := New(100, 10)
	for i := 0; i < 50; i++ {
		f1.Add([]byte(fmt.Sprintf("key%d", i)))
	}

	// Create filter from existing data
	data := f1.Encode()
	decoded, _ := Decode(data)

	f2 := NewWithData(decoded.bits, decoded.k)

	// Should behave identically
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		if f1.MayContain(key) != f2.MayContain(key) {
			t.Errorf("filters should behave identically for key%d", i)
		}
	}
}

func TestHashDeterministic(t *testing.T) {
	key := []byte("test-key")

	h1 := hash(key)
	h2 := hash(key)

	if h1 != h2 {
		t.Error("hash should be deterministic")
	}
}

func TestHashDistribution(t *testing.T) {
	// Test that hash produces different values for different keys
	hashes := make(map[uint32]bool)

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		h := hash(key)
		hashes[h] = true
	}

	// Should have many unique hashes (not all due to collisions, but most)
	if len(hashes) < 950 {
		t.Errorf("hash distribution seems poor: only %d unique hashes for 1000 keys", len(hashes))
	}
}

// Benchmarks

func BenchmarkFilterAdd(b *testing.B) {
	f := New(b.N, 10)
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Add(keys[i])
	}
}

func BenchmarkFilterMayContain(b *testing.B) {
	f := New(10000, 10)
	for i := 0; i < 10000; i++ {
		f.Add([]byte(fmt.Sprintf("key%08d", i)))
	}

	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = []byte(fmt.Sprintf("key%08d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.MayContain(keys[i%1000])
	}
}

func BenchmarkFilterEncode(b *testing.B) {
	f := New(10000, 10)
	for i := 0; i < 10000; i++ {
		f.Add([]byte(fmt.Sprintf("key%08d", i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Encode()
	}
}

func BenchmarkFilterDecode(b *testing.B) {
	f := New(10000, 10)
	for i := 0; i < 10000; i++ {
		f.Add([]byte(fmt.Sprintf("key%08d", i)))
	}
	data := f.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Decode(data)
	}
}
