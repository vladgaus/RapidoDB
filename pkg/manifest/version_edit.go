// Package manifest provides crash recovery through persistent metadata.
//
// The manifest system tracks which SSTable files exist and their levels,
// enabling the database to recover its state after a crash.
//
// Architecture:
//
//	┌─────────────────────────────────────────────────────────────┐
//	│                     Manifest System                          │
//	├─────────────────────────────────────────────────────────────┤
//	│                                                              │
//	│  CURRENT file ──────► Points to active MANIFEST              │
//	│       │                                                      │
//	│       ▼                                                      │
//	│  MANIFEST-000001                                             │
//	│  ┌─────────────────────────────────────────────────────┐    │
//	│  │ VersionEdit 1: Comparator="bytewise", NextFile=1    │    │
//	│  │ VersionEdit 2: AddFile(L0, file1), LogNum=2         │    │
//	│  │ VersionEdit 3: AddFile(L0, file2)                   │    │
//	│  │ VersionEdit 4: DeleteFile(L0, file1), AddFile(L1,..)│    │
//	│  │ ...                                                  │    │
//	│  └─────────────────────────────────────────────────────┘    │
//	│                                                              │
//	│  On Recovery:                                                │
//	│  1. Read CURRENT to find manifest                           │
//	│  2. Replay all VersionEdits to rebuild state                │
//	│  3. Open WAL files >= LogNumber to recover MemTable         │
//	│                                                              │
//	└─────────────────────────────────────────────────────────────┘
//
// VersionEdit Format (binary):
//
//	┌──────────┬──────────┬──────────────────────┐
//	│ Tag (1B) │ Len (4B) │ Data (variable)      │
//	└──────────┴──────────┴──────────────────────┘
//
// Tags:
//   - TagComparator (1): Comparator name
//   - TagLogNumber (2): WAL log number
//   - TagNextFileNumber (3): Next file number to allocate
//   - TagLastSequence (4): Last sequence number
//   - TagDeletedFile (6): File removed (level + filenum)
//   - TagNewFile (7): File added (level + metadata)
package manifest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Record tags for VersionEdit fields
const (
	TagComparator     byte = 1
	TagLogNumber      byte = 2
	TagNextFileNumber byte = 3
	TagLastSequence   byte = 4
	TagCompactPointer byte = 5 // Not used, but reserved
	TagDeletedFile    byte = 6
	TagNewFile        byte = 7
	TagPrevLogNumber  byte = 9 // Not used, but reserved
)

// FileMeta contains metadata about an SSTable file.
type FileMeta struct {
	FileNum uint64
	Size    int64
	MinKey  []byte
	MaxKey  []byte
	MinSeq  uint64
	MaxSeq  uint64
	NumKeys int64
}

// Clone creates a deep copy of FileMeta.
func (f *FileMeta) Clone() *FileMeta {
	return &FileMeta{
		FileNum: f.FileNum,
		Size:    f.Size,
		MinKey:  append([]byte{}, f.MinKey...),
		MaxKey:  append([]byte{}, f.MaxKey...),
		MinSeq:  f.MinSeq,
		MaxSeq:  f.MaxSeq,
		NumKeys: f.NumKeys,
	}
}

// LevelFile represents a file at a specific level.
type LevelFile struct {
	Level int
	Meta  *FileMeta
}

// DeletedFile represents a file to be removed.
type DeletedFile struct {
	Level   int
	FileNum uint64
}

// VersionEdit represents a set of changes to the database state.
// Each edit is written atomically to the manifest file.
type VersionEdit struct {
	// Comparator name (only set in first edit)
	Comparator    string
	HasComparator bool

	// WAL log number - files < this can be deleted
	LogNumber    uint64
	HasLogNumber bool

	// Next file number to allocate
	NextFileNumber    uint64
	HasNextFileNumber bool

	// Last sequence number written
	LastSequence    uint64
	HasLastSequence bool

	// Files to add (level + metadata)
	NewFiles []LevelFile

	// Files to delete (level + filenum)
	DeletedFiles []DeletedFile
}

// NewVersionEdit creates an empty version edit.
func NewVersionEdit() *VersionEdit {
	return &VersionEdit{}
}

// SetComparator sets the comparator name.
func (ve *VersionEdit) SetComparator(name string) {
	ve.Comparator = name
	ve.HasComparator = true
}

// SetLogNumber sets the minimum WAL log number.
func (ve *VersionEdit) SetLogNumber(num uint64) {
	ve.LogNumber = num
	ve.HasLogNumber = true
}

// SetNextFileNumber sets the next file number.
func (ve *VersionEdit) SetNextFileNumber(num uint64) {
	ve.NextFileNumber = num
	ve.HasNextFileNumber = true
}

// SetLastSequence sets the last sequence number.
func (ve *VersionEdit) SetLastSequence(seq uint64) {
	ve.LastSequence = seq
	ve.HasLastSequence = true
}

// AddFile adds a new file to a level.
func (ve *VersionEdit) AddFile(level int, meta *FileMeta) {
	ve.NewFiles = append(ve.NewFiles, LevelFile{
		Level: level,
		Meta:  meta.Clone(),
	})
}

// DeleteFile marks a file for deletion.
func (ve *VersionEdit) DeleteFile(level int, fileNum uint64) {
	ve.DeletedFiles = append(ve.DeletedFiles, DeletedFile{
		Level:   level,
		FileNum: fileNum,
	})
}

// Encode serializes the VersionEdit to bytes.
func (ve *VersionEdit) Encode() []byte {
	var buf bytes.Buffer

	if ve.HasComparator {
		ve.encodeString(&buf, TagComparator, ve.Comparator)
	}
	if ve.HasLogNumber {
		ve.encodeUint64(&buf, TagLogNumber, ve.LogNumber)
	}
	if ve.HasNextFileNumber {
		ve.encodeUint64(&buf, TagNextFileNumber, ve.NextFileNumber)
	}
	if ve.HasLastSequence {
		ve.encodeUint64(&buf, TagLastSequence, ve.LastSequence)
	}

	for _, df := range ve.DeletedFiles {
		ve.encodeDeletedFile(&buf, df)
	}

	for _, nf := range ve.NewFiles {
		ve.encodeNewFile(&buf, nf)
	}

	return buf.Bytes()
}

func (ve *VersionEdit) encodeString(buf *bytes.Buffer, tag byte, s string) {
	buf.WriteByte(tag)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(s))); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}
	buf.WriteString(s)
}

func (ve *VersionEdit) encodeUint64(buf *bytes.Buffer, tag byte, v uint64) {
	buf.WriteByte(tag)
	if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}
}

func (ve *VersionEdit) encodeDeletedFile(buf *bytes.Buffer, df DeletedFile) {
	buf.WriteByte(TagDeletedFile)
	if err := binary.Write(buf, binary.LittleEndian, uint32(df.Level)); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}
	if err := binary.Write(buf, binary.LittleEndian, df.FileNum); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}
}

func (ve *VersionEdit) encodeNewFile(buf *bytes.Buffer, lf LevelFile) {
	buf.WriteByte(TagNewFile)
	if err := binary.Write(buf, binary.LittleEndian, uint32(lf.Level)); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}
	if err := binary.Write(buf, binary.LittleEndian, lf.Meta.FileNum); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}
	if err := binary.Write(buf, binary.LittleEndian, lf.Meta.Size); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}

	// MinKey
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(lf.Meta.MinKey))); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}
	buf.Write(lf.Meta.MinKey)

	// MaxKey
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(lf.Meta.MaxKey))); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}
	buf.Write(lf.Meta.MaxKey)

	// Sequence numbers
	if err := binary.Write(buf, binary.LittleEndian, lf.Meta.MinSeq); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}
	if err := binary.Write(buf, binary.LittleEndian, lf.Meta.MaxSeq); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}

	// NumKeys
	if err := binary.Write(buf, binary.LittleEndian, lf.Meta.NumKeys); err != nil {
		panic("binary.Write to bytes.Buffer failed: " + err.Error())
	}
}

// Decode deserializes a VersionEdit from bytes.
func (ve *VersionEdit) Decode(data []byte) error {
	r := bytes.NewReader(data)

	for r.Len() > 0 {
		tag, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read tag: %w", err)
		}

		switch tag {
		case TagComparator:
			s, err := ve.decodeString(r)
			if err != nil {
				return fmt.Errorf("decode comparator: %w", err)
			}
			ve.Comparator = s
			ve.HasComparator = true

		case TagLogNumber:
			v, err := ve.decodeUint64(r)
			if err != nil {
				return fmt.Errorf("decode log number: %w", err)
			}
			ve.LogNumber = v
			ve.HasLogNumber = true

		case TagNextFileNumber:
			v, err := ve.decodeUint64(r)
			if err != nil {
				return fmt.Errorf("decode next file number: %w", err)
			}
			ve.NextFileNumber = v
			ve.HasNextFileNumber = true

		case TagLastSequence:
			v, err := ve.decodeUint64(r)
			if err != nil {
				return fmt.Errorf("decode last sequence: %w", err)
			}
			ve.LastSequence = v
			ve.HasLastSequence = true

		case TagDeletedFile:
			df, err := ve.decodeDeletedFile(r)
			if err != nil {
				return fmt.Errorf("decode deleted file: %w", err)
			}
			ve.DeletedFiles = append(ve.DeletedFiles, df)

		case TagNewFile:
			lf, err := ve.decodeNewFile(r)
			if err != nil {
				return fmt.Errorf("decode new file: %w", err)
			}
			ve.NewFiles = append(ve.NewFiles, lf)

		case TagCompactPointer, TagPrevLogNumber:
			// Skip deprecated tags
			if err := ve.skipTag(r, tag); err != nil {
				return err
			}

		default:
			return fmt.Errorf("unknown tag: %d", tag)
		}
	}

	return nil
}

func (ve *VersionEdit) decodeString(r *bytes.Reader) (string, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return "", err
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}
	return string(data), nil
}

func (ve *VersionEdit) decodeUint64(r *bytes.Reader) (uint64, error) {
	var v uint64
	if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
		return 0, err
	}
	return v, nil
}

func (ve *VersionEdit) decodeDeletedFile(r *bytes.Reader) (DeletedFile, error) {
	var level uint32
	var fileNum uint64

	if err := binary.Read(r, binary.LittleEndian, &level); err != nil {
		return DeletedFile{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &fileNum); err != nil {
		return DeletedFile{}, err
	}

	return DeletedFile{Level: int(level), FileNum: fileNum}, nil
}

func (ve *VersionEdit) decodeNewFile(r *bytes.Reader) (LevelFile, error) {
	var level uint32
	if err := binary.Read(r, binary.LittleEndian, &level); err != nil {
		return LevelFile{}, err
	}

	meta := &FileMeta{}

	if err := binary.Read(r, binary.LittleEndian, &meta.FileNum); err != nil {
		return LevelFile{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &meta.Size); err != nil {
		return LevelFile{}, err
	}

	// MinKey
	var minKeyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &minKeyLen); err != nil {
		return LevelFile{}, err
	}
	meta.MinKey = make([]byte, minKeyLen)
	if _, err := io.ReadFull(r, meta.MinKey); err != nil {
		return LevelFile{}, err
	}

	// MaxKey
	var maxKeyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &maxKeyLen); err != nil {
		return LevelFile{}, err
	}
	meta.MaxKey = make([]byte, maxKeyLen)
	if _, err := io.ReadFull(r, meta.MaxKey); err != nil {
		return LevelFile{}, err
	}

	// Sequence numbers
	if err := binary.Read(r, binary.LittleEndian, &meta.MinSeq); err != nil {
		return LevelFile{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &meta.MaxSeq); err != nil {
		return LevelFile{}, err
	}

	// NumKeys
	if err := binary.Read(r, binary.LittleEndian, &meta.NumKeys); err != nil {
		return LevelFile{}, err
	}

	return LevelFile{Level: int(level), Meta: meta}, nil
}

func (ve *VersionEdit) skipTag(r *bytes.Reader, tag byte) error {
	switch tag {
	case TagCompactPointer:
		// level (4) + key length (4) + key
		var level uint32
		if err := binary.Read(r, binary.LittleEndian, &level); err != nil {
			return err
		}
		var keyLen uint32
		if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
			return err
		}
		if _, err := r.Seek(int64(keyLen), io.SeekCurrent); err != nil {
			return err
		}
	case TagPrevLogNumber:
		var v uint64
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return err
		}
	}
	return nil
}

// String returns a human-readable representation.
func (ve *VersionEdit) String() string {
	var buf bytes.Buffer
	buf.WriteString("VersionEdit{")

	if ve.HasComparator {
		fmt.Fprintf(&buf, "Comparator=%q, ", ve.Comparator)
	}
	if ve.HasLogNumber {
		fmt.Fprintf(&buf, "LogNumber=%d, ", ve.LogNumber)
	}
	if ve.HasNextFileNumber {
		fmt.Fprintf(&buf, "NextFileNumber=%d, ", ve.NextFileNumber)
	}
	if ve.HasLastSequence {
		fmt.Fprintf(&buf, "LastSequence=%d, ", ve.LastSequence)
	}

	if len(ve.DeletedFiles) > 0 {
		buf.WriteString("DeletedFiles=[")
		for i, df := range ve.DeletedFiles {
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "L%d:%d", df.Level, df.FileNum)
		}
		buf.WriteString("], ")
	}

	if len(ve.NewFiles) > 0 {
		buf.WriteString("NewFiles=[")
		for i, nf := range ve.NewFiles {
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "L%d:%d(%d bytes)", nf.Level, nf.Meta.FileNum, nf.Meta.Size)
		}
		buf.WriteString("]")
	}

	buf.WriteString("}")
	return buf.String()
}
