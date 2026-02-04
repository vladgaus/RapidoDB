// Package bloom provides a Bloom filter implementation.
//
// A Bloom filter is a space-efficient probabilistic data structure that
// tests whether an element is a member of a set. False positives are
// possible, but false negatives are not.
//
// This implementation uses double hashing to simulate multiple hash
// functions efficiently.
//
// Usage:
//
//	// Create filter for ~1000 keys with 10 bits/key (~1% false positive rate)
//	f := bloom.New(1000, 10)
//
//	// Add keys
//	f.Add([]byte("key1"))
//	f.Add([]byte("key2"))
//
//	// Check membership
//	f.MayContain([]byte("key1")) // true
//	f.MayContain([]byte("key3")) // probably false
//
//	// Serialize for storage
//	data := f.Encode()
//
//	// Deserialize
//	f2, _ := bloom.Decode(data)
package bloom

// Filter is a Bloom filter for fast set membership testing.
type Filter struct {
	bits []byte // Bit array
	k    uint8  // Number of hash functions
}

// New creates a new Bloom filter.
//
// Parameters:
//   - numKeys: Expected number of keys to add
//   - bitsPerKey: Bits per key (10 gives ~1% false positive rate)
//
// The false positive rate is approximately (1 - e^(-k*n/m))^k where:
//   - k = number of hash functions
//   - n = number of keys
//   - m = number of bits
//
// Common configurations:
//   - 10 bits/key → ~1% false positive rate
//   - 15 bits/key → ~0.1% false positive rate
//   - 20 bits/key → ~0.01% false positive rate
func New(numKeys, bitsPerKey int) *Filter {
	if numKeys < 1 {
		numKeys = 1
	}
	if bitsPerKey < 1 {
		bitsPerKey = 10
	}

	// Calculate size
	numBits := numKeys * bitsPerKey
	if numBits < 64 {
		numBits = 64 // Minimum size
	}

	// Round up to byte boundary
	numBytes := (numBits + 7) / 8
	numBits = numBytes * 8 // Actual bits available

	// Calculate optimal number of hash functions: k = (m/n) * ln(2) ≈ 0.69 * m/n
	k := uint8((numBits / numKeys) * 69 / 100)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	return &Filter{
		bits: make([]byte, numBytes),
		k:    k,
	}
}

// NewWithData creates a Filter from existing bit data.
// Used for deserialization.
func NewWithData(bits []byte, k uint8) *Filter {
	return &Filter{
		bits: bits,
		k:    k,
	}
}

// Add adds a key to the filter.
func (f *Filter) Add(key []byte) {
	numBits := uint32(len(f.bits) * 8)
	h := hash(key)
	delta := (h >> 17) | (h << 15) // Rotate right 17 bits

	for j := uint8(0); j < f.k; j++ {
		bitpos := h % numBits
		f.bits[bitpos/8] |= 1 << (bitpos % 8)
		h += delta
	}
}

// MayContain returns true if the key might be in the set.
// False positives are possible, but false negatives are not.
func (f *Filter) MayContain(key []byte) bool {
	if len(f.bits) == 0 {
		return false
	}

	numBits := uint32(len(f.bits) * 8)
	h := hash(key)
	delta := (h >> 17) | (h << 15)

	for j := uint8(0); j < f.k; j++ {
		bitpos := h % numBits
		if f.bits[bitpos/8]&(1<<(bitpos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

// Encode serializes the filter to bytes.
//
// Format: [bits...][k (1 byte)]
func (f *Filter) Encode() []byte {
	data := make([]byte, len(f.bits)+1)
	copy(data, f.bits)
	data[len(data)-1] = f.k
	return data
}

// Decode deserializes a filter from bytes.
func Decode(data []byte) (*Filter, error) {
	if len(data) < 2 { // At least 1 byte of bits + k
		return nil, ErrInvalidFilter
	}

	k := data[len(data)-1]
	bits := make([]byte, len(data)-1)
	copy(bits, data[:len(data)-1])

	return &Filter{
		bits: bits,
		k:    k,
	}, nil
}

// Size returns the size of the filter in bytes.
func (f *Filter) Size() int {
	return len(f.bits)
}

// NumHashes returns the number of hash functions used.
func (f *Filter) NumHashes() int {
	return int(f.k)
}

// EstimateFalsePositiveRate estimates the false positive rate
// for the given number of keys.
func (f *Filter) EstimateFalsePositiveRate(numKeys int) float64 {
	if numKeys <= 0 || len(f.bits) == 0 {
		return 1.0
	}

	m := float64(len(f.bits) * 8) // number of bits
	n := float64(numKeys)         // number of keys
	k := float64(f.k)             // number of hash functions

	// FP rate ≈ (1 - e^(-kn/m))^k
	// Using math would be cleaner but avoiding import for simplicity
	// Approximation: (1 - (1 - 1/m)^(kn))^k ≈ (kn/m)^k for small kn/m

	ratio := k * n / m
	if ratio > 1 {
		return 1.0
	}

	// Simple approximation
	fp := 1.0
	for i := 0; i < int(k); i++ {
		fp *= ratio
	}
	return fp
}

// hash computes a 32-bit hash using a MurmurHash variant.
func hash(key []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)

	h := uint32(seed) ^ uint32(len(key))*m

	// Process 4 bytes at a time
	for len(key) >= 4 {
		h += uint32(key[0]) | uint32(key[1])<<8 | uint32(key[2])<<16 | uint32(key[3])<<24
		h *= m
		h ^= h >> 16
		key = key[4:]
	}

	// Process remaining bytes
	switch len(key) {
	case 3:
		h += uint32(key[2]) << 16
		fallthrough
	case 2:
		h += uint32(key[1]) << 8
		fallthrough
	case 1:
		h += uint32(key[0])
		h *= m
		h ^= h >> 24
	}

	return h
}

// Errors
var (
	ErrInvalidFilter = &bloomError{"invalid filter data"}
)

type bloomError struct {
	msg string
}

func (e *bloomError) Error() string {
	return "bloom: " + e.msg
}
