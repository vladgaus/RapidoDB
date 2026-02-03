package encoding

import (
	"bytes"
	"testing"

	"github.com/rapidodb/rapidodb/pkg/types"
)

func TestPutGetUint16(t *testing.T) {
	tests := []uint16{0, 1, 255, 256, 65535}
	buf := make([]byte, 2)

	for _, v := range tests {
		PutUint16(buf, v)
		got := GetUint16(buf)
		if got != v {
			t.Errorf("uint16 roundtrip failed: expected %d, got %d", v, got)
		}
	}
}

func TestPutGetUint32(t *testing.T) {
	tests := []uint32{0, 1, 255, 256, 65535, 1<<24, 1<<32 - 1}
	buf := make([]byte, 4)

	for _, v := range tests {
		PutUint32(buf, v)
		got := GetUint32(buf)
		if got != v {
			t.Errorf("uint32 roundtrip failed: expected %d, got %d", v, got)
		}
	}
}

func TestPutGetUint64(t *testing.T) {
	tests := []uint64{0, 1, 255, 256, 65535, 1 << 24, 1 << 32, 1 << 48, 1<<64 - 1}
	buf := make([]byte, 8)

	for _, v := range tests {
		PutUint64(buf, v)
		got := GetUint64(buf)
		if got != v {
			t.Errorf("uint64 roundtrip failed: expected %d, got %d", v, got)
		}
	}
}

func TestVarint(t *testing.T) {
	tests := []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, 1 << 21, 1 << 28, 1 << 35, 1 << 63}
	buf := make([]byte, 10)

	for _, v := range tests {
		n := PutVarint(buf, v)
		got, m := GetVarint(buf)

		if n != m {
			t.Errorf("varint length mismatch for %d: wrote %d bytes, read %d bytes", v, n, m)
		}

		if got != v {
			t.Errorf("varint roundtrip failed: expected %d, got %d", v, got)
		}

		expectedLen := VarintLen(v)
		if n != expectedLen {
			t.Errorf("VarintLen mismatch for %d: expected %d, got %d", v, expectedLen, n)
		}
	}
}

func TestEncodeDecodeEntry(t *testing.T) {
	tests := []*types.Entry{
		{
			Key:    []byte("key1"),
			Value:  []byte("value1"),
			Type:   types.EntryTypePut,
			SeqNum: 1,
		},
		{
			Key:    []byte("key2"),
			Value:  []byte(""),
			Type:   types.EntryTypePut,
			SeqNum: 100,
		},
		{
			Key:    []byte("deleted-key"),
			Value:  nil,
			Type:   types.EntryTypeDelete,
			SeqNum: 999,
		},
		{
			Key:    []byte("big-value-key"),
			Value:  bytes.Repeat([]byte("x"), 10000),
			Type:   types.EntryTypePut,
			SeqNum: 12345678,
		},
	}

	for i, e := range tests {
		encoded := EncodeEntry(e)

		// Check size calculation
		expectedSize := EntryEncodedSize(e)
		if len(encoded) != expectedSize {
			t.Errorf("test %d: encoded size mismatch: expected %d, got %d", i, expectedSize, len(encoded))
		}

		// Decode
		decoded, n, err := DecodeEntry(encoded)
		if err != nil {
			t.Errorf("test %d: decode error: %v", i, err)
			continue
		}

		if n != len(encoded) {
			t.Errorf("test %d: bytes consumed mismatch: expected %d, got %d", i, len(encoded), n)
		}

		// Verify fields
		if decoded.Type != e.Type {
			t.Errorf("test %d: type mismatch: expected %v, got %v", i, e.Type, decoded.Type)
		}

		if decoded.SeqNum != e.SeqNum {
			t.Errorf("test %d: seqnum mismatch: expected %d, got %d", i, e.SeqNum, decoded.SeqNum)
		}

		if !bytes.Equal(decoded.Key, e.Key) {
			t.Errorf("test %d: key mismatch: expected %v, got %v", i, e.Key, decoded.Key)
		}

		if !bytes.Equal(decoded.Value, e.Value) {
			t.Errorf("test %d: value mismatch: expected %v, got %v", i, e.Value, decoded.Value)
		}
	}
}

func TestEncodeEntryTo(t *testing.T) {
	entry := &types.Entry{
		Key:    []byte("test-key"),
		Value:  []byte("test-value"),
		Type:   types.EntryTypePut,
		SeqNum: 42,
	}

	buf := make([]byte, EntryEncodedSize(entry))
	n := EncodeEntryTo(buf, entry)

	if n != len(buf) {
		t.Errorf("EncodeEntryTo returned wrong length: expected %d, got %d", len(buf), n)
	}

	// Verify by decoding
	decoded, _, err := DecodeEntry(buf)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if !bytes.Equal(decoded.Key, entry.Key) {
		t.Errorf("key mismatch after EncodeEntryTo")
	}
}

func TestDecodeEntryInsufficientData(t *testing.T) {
	// Test with insufficient header data
	_, _, err := DecodeEntry(make([]byte, 10))
	if err == nil {
		t.Error("expected error for insufficient header data")
	}

	// Test with insufficient key/value data
	entry := &types.Entry{
		Key:    []byte("key"),
		Value:  []byte("value"),
		Type:   types.EntryTypePut,
		SeqNum: 1,
	}
	encoded := EncodeEntry(entry)

	// Truncate data
	_, _, err = DecodeEntry(encoded[:len(encoded)-1])
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestChecksum(t *testing.T) {
	data := []byte("hello, world!")

	// Same data should produce same checksum
	c1 := Checksum(data)
	c2 := Checksum(data)
	if c1 != c2 {
		t.Error("same data should produce same checksum")
	}

	// Different data should produce different checksum
	c3 := Checksum([]byte("different data"))
	if c1 == c3 {
		t.Error("different data should produce different checksum")
	}

	// Verify checksum
	if !VerifyChecksum(data, c1) {
		t.Error("checksum verification failed")
	}

	if VerifyChecksum(data, c1+1) {
		t.Error("checksum verification should fail for wrong checksum")
	}
}

func TestAppendChecksum(t *testing.T) {
	data := []byte("test data")
	withChecksum := AppendChecksum(data)

	// Should be 4 bytes longer
	if len(withChecksum) != len(data)+4 {
		t.Errorf("expected length %d, got %d", len(data)+4, len(withChecksum))
	}

	// Extract and verify checksum
	storedChecksum := GetUint32(withChecksum[len(data):])
	calculatedChecksum := Checksum(data)

	if storedChecksum != calculatedChecksum {
		t.Error("appended checksum doesn't match calculated checksum")
	}
}

func TestInternalKeyEncodeDecode(t *testing.T) {
	tests := []types.InternalKey{
		{UserKey: []byte("key1"), SeqNum: 1, Type: types.EntryTypePut},
		{UserKey: []byte("key2"), SeqNum: 100, Type: types.EntryTypeDelete},
		{UserKey: []byte(""), SeqNum: 0, Type: types.EntryTypePut},
		{UserKey: bytes.Repeat([]byte("x"), 1000), SeqNum: 1 << 63, Type: types.EntryTypeDelete},
	}

	for i, ik := range tests {
		encoded := EncodeInternalKey(ik)
		decoded, err := DecodeInternalKey(encoded)
		if err != nil {
			t.Errorf("test %d: decode error: %v", i, err)
			continue
		}

		if !bytes.Equal(decoded.UserKey, ik.UserKey) {
			t.Errorf("test %d: UserKey mismatch", i)
		}

		if decoded.SeqNum != ik.SeqNum {
			t.Errorf("test %d: SeqNum mismatch: expected %d, got %d", i, ik.SeqNum, decoded.SeqNum)
		}

		if decoded.Type != ik.Type {
			t.Errorf("test %d: Type mismatch: expected %v, got %v", i, ik.Type, decoded.Type)
		}
	}
}

func BenchmarkEncodeEntry(b *testing.B) {
	entry := &types.Entry{
		Key:    bytes.Repeat([]byte("k"), 16),
		Value:  bytes.Repeat([]byte("v"), 100),
		Type:   types.EntryTypePut,
		SeqNum: 12345,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		EncodeEntry(entry)
	}
}

func BenchmarkDecodeEntry(b *testing.B) {
	entry := &types.Entry{
		Key:    bytes.Repeat([]byte("k"), 16),
		Value:  bytes.Repeat([]byte("v"), 100),
		Type:   types.EntryTypePut,
		SeqNum: 12345,
	}
	encoded := EncodeEntry(entry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DecodeEntry(encoded)
	}
}

func BenchmarkChecksum(b *testing.B) {
	data := bytes.Repeat([]byte("x"), 4096)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Checksum(data)
	}
}
