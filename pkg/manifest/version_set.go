package manifest

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// MaxLevels is the maximum number of levels in the LSM tree.
const MaxLevels = 7

// Version represents a consistent view of the database files.
// Versions are immutable once created.
type Version struct {
	// Files at each level
	Files [MaxLevels][]*FileMeta

	// Reference count for safe deletion
	refs int
}

// NewVersion creates an empty version.
func NewVersion() *Version {
	return &Version{refs: 1}
}

// Ref increments the reference count.
func (v *Version) Ref() {
	v.refs++
}

// Unref decrements the reference count.
func (v *Version) Unref() {
	v.refs--
}

// NumFiles returns the number of files at a level.
func (v *Version) NumFiles(level int) int {
	if level < 0 || level >= MaxLevels {
		return 0
	}
	return len(v.Files[level])
}

// GetFiles returns a copy of files at a level.
func (v *Version) GetFiles(level int) []*FileMeta {
	if level < 0 || level >= MaxLevels {
		return nil
	}
	result := make([]*FileMeta, len(v.Files[level]))
	for i, f := range v.Files[level] {
		result[i] = f.Clone()
	}
	return result
}

// VersionSet manages versions and the manifest file.
type VersionSet struct {
	mu sync.RWMutex

	// Database directory
	dbDir string

	// Current version
	current *Version

	// Manifest state
	manifestNum    uint64
	manifestWriter *Writer

	// Counters
	nextFileNumber uint64
	lastSequence   uint64
	logNumber      uint64

	// Comparator name
	comparator string
}

// NewVersionSet creates a new version set.
func NewVersionSet(dbDir string) *VersionSet {
	return &VersionSet{
		dbDir:          dbDir,
		current:        NewVersion(),
		nextFileNumber: 1,
		comparator:     "bytewise",
	}
}

// Current returns the current version.
func (vs *VersionSet) Current() *Version {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.current
}

// NextFileNumber returns and increments the next file number.
func (vs *VersionSet) NextFileNumber() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	num := vs.nextFileNumber
	vs.nextFileNumber++
	return num
}

// MarkFileNumberUsed ensures the next file number is higher than num.
func (vs *VersionSet) MarkFileNumberUsed(num uint64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if num >= vs.nextFileNumber {
		vs.nextFileNumber = num + 1
	}
}

// LastSequence returns the last sequence number.
func (vs *VersionSet) LastSequence() uint64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.lastSequence
}

// SetLastSequence sets the last sequence number.
func (vs *VersionSet) SetLastSequence(seq uint64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if seq > vs.lastSequence {
		vs.lastSequence = seq
	}
}

// LogNumber returns the current log number.
func (vs *VersionSet) LogNumber() uint64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.logNumber
}

// SetLogNumber sets the current log number.
func (vs *VersionSet) SetLogNumber(num uint64) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	vs.logNumber = num
}

// Recover loads the version set from the manifest.
func (vs *VersionSet) Recover() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Read CURRENT file
	manifestName, err := ReadCurrent(vs.dbDir)
	if err != nil {
		if os.IsNotExist(err) {
			// No manifest exists, start fresh
			return nil
		}
		return fmt.Errorf("read current: %w", err)
	}

	// Parse manifest number
	manifestNum, ok := ParseManifestNumber(manifestName)
	if !ok {
		return fmt.Errorf("invalid manifest name: %s", manifestName)
	}
	vs.manifestNum = manifestNum

	// Read manifest file
	manifestPath := filepath.Join(vs.dbDir, manifestName)
	reader, err := NewReader(manifestPath)
	if err != nil {
		return fmt.Errorf("open manifest: %w", err)
	}
	defer reader.Close()

	// Build version by replaying all edits
	builder := NewVersionBuilder(vs.current)

	for {
		edit, err := reader.ReadEdit()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read edit: %w", err)
		}

		// Apply edit to builder
		if err := builder.Apply(edit); err != nil {
			return fmt.Errorf("apply edit: %w", err)
		}

		// Update counters
		if edit.HasComparator {
			vs.comparator = edit.Comparator
		}
		if edit.HasNextFileNumber && edit.NextFileNumber > vs.nextFileNumber {
			vs.nextFileNumber = edit.NextFileNumber
		}
		if edit.HasLastSequence && edit.LastSequence > vs.lastSequence {
			vs.lastSequence = edit.LastSequence
		}
		if edit.HasLogNumber && edit.LogNumber > vs.logNumber {
			vs.logNumber = edit.LogNumber
		}
	}

	// Finalize new version
	vs.current = builder.Build()

	return nil
}

// LogAndApply atomically logs an edit and applies it to the current version.
func (vs *VersionSet) LogAndApply(edit *VersionEdit) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Create new version by applying edit
	builder := NewVersionBuilder(vs.current)
	if err := builder.Apply(edit); err != nil {
		return fmt.Errorf("apply edit: %w", err)
	}
	newVersion := builder.Build()

	// Ensure manifest writer exists
	if err := vs.ensureManifestWriter(edit); err != nil {
		return err
	}

	// Write edit to manifest
	if err := vs.manifestWriter.WriteEdit(edit); err != nil {
		return fmt.Errorf("write edit: %w", err)
	}

	// Sync manifest
	if err := vs.manifestWriter.Sync(); err != nil {
		return fmt.Errorf("sync manifest: %w", err)
	}

	// Update counters
	if edit.HasNextFileNumber && edit.NextFileNumber > vs.nextFileNumber {
		vs.nextFileNumber = edit.NextFileNumber
	}
	if edit.HasLastSequence && edit.LastSequence > vs.lastSequence {
		vs.lastSequence = edit.LastSequence
	}
	if edit.HasLogNumber && edit.LogNumber > vs.logNumber {
		vs.logNumber = edit.LogNumber
	}

	// Install new version
	vs.current.Unref()
	vs.current = newVersion

	return nil
}

// ensureManifestWriter ensures the manifest writer is open.
// Creates a new manifest if needed.
func (vs *VersionSet) ensureManifestWriter(edit *VersionEdit) error {
	if vs.manifestWriter != nil {
		return nil
	}

	// Allocate new manifest number
	vs.manifestNum = vs.nextFileNumber
	vs.nextFileNumber++

	manifestPath := filepath.Join(vs.dbDir, ManifestFileName(vs.manifestNum))

	// Create manifest writer
	writer, err := NewWriter(manifestPath)
	if err != nil {
		return fmt.Errorf("create manifest: %w", err)
	}
	vs.manifestWriter = writer

	// Write snapshot of current state
	snapshot := vs.buildSnapshot()
	if err := writer.WriteEdit(snapshot); err != nil {
		writer.Close()
		vs.manifestWriter = nil
		return fmt.Errorf("write snapshot: %w", err)
	}

	// Update CURRENT file
	if err := SetCurrent(vs.dbDir, vs.manifestNum); err != nil {
		writer.Close()
		vs.manifestWriter = nil
		return fmt.Errorf("set current: %w", err)
	}

	return nil
}

// buildSnapshot creates a VersionEdit containing the full current state.
func (vs *VersionSet) buildSnapshot() *VersionEdit {
	edit := NewVersionEdit()

	edit.SetComparator(vs.comparator)
	edit.SetNextFileNumber(vs.nextFileNumber)
	edit.SetLastSequence(vs.lastSequence)
	edit.SetLogNumber(vs.logNumber)

	// Add all files
	for level := 0; level < MaxLevels; level++ {
		for _, f := range vs.current.Files[level] {
			edit.AddFile(level, f)
		}
	}

	return edit
}

// Close closes the version set.
func (vs *VersionSet) Close() error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.manifestWriter != nil {
		if err := vs.manifestWriter.Close(); err != nil {
			return err
		}
		vs.manifestWriter = nil
	}

	return nil
}

// ManifestFileNumber returns the current manifest file number.
func (vs *VersionSet) ManifestFileNumber() uint64 {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.manifestNum
}

// VersionBuilder builds a new Version by applying edits.
type VersionBuilder struct {
	base    *Version
	added   [MaxLevels]map[uint64]*FileMeta // Files to add
	deleted [MaxLevels]map[uint64]struct{}  // Files to delete
}

// NewVersionBuilder creates a new version builder.
func NewVersionBuilder(base *Version) *VersionBuilder {
	vb := &VersionBuilder{base: base}
	for i := 0; i < MaxLevels; i++ {
		vb.added[i] = make(map[uint64]*FileMeta)
		vb.deleted[i] = make(map[uint64]struct{})
	}
	return vb
}

// Apply applies a VersionEdit to the builder.
func (vb *VersionBuilder) Apply(edit *VersionEdit) error {
	// Process deletions first
	for _, df := range edit.DeletedFiles {
		if df.Level < 0 || df.Level >= MaxLevels {
			return fmt.Errorf("invalid level %d for deleted file", df.Level)
		}
		vb.deleted[df.Level][df.FileNum] = struct{}{}
		// Also remove from added (file might have been added in earlier edit)
		delete(vb.added[df.Level], df.FileNum)
	}

	// Process additions
	for _, nf := range edit.NewFiles {
		if nf.Level < 0 || nf.Level >= MaxLevels {
			return fmt.Errorf("invalid level %d for new file", nf.Level)
		}
		// Remove from deleted if it was there (re-adding a file)
		delete(vb.deleted[nf.Level], nf.Meta.FileNum)
		// Add to added
		vb.added[nf.Level][nf.Meta.FileNum] = nf.Meta.Clone()
	}

	return nil
}

// Build creates the final Version.
func (vb *VersionBuilder) Build() *Version {
	v := NewVersion()

	for level := 0; level < MaxLevels; level++ {
		// Start with base files (excluding deleted)
		for _, f := range vb.base.Files[level] {
			if _, deleted := vb.deleted[level][f.FileNum]; !deleted {
				v.Files[level] = append(v.Files[level], f.Clone())
			}
		}

		// Add new files
		for _, f := range vb.added[level] {
			v.Files[level] = append(v.Files[level], f.Clone())
		}

		// Sort L1+ by MinKey (L0 stays in insertion order)
		if level > 0 {
			sortFilesByMinKey(v.Files[level])
		}
	}

	return v
}

// sortFilesByMinKey sorts files by their minimum key.
func sortFilesByMinKey(files []*FileMeta) {
	// Simple insertion sort (files are usually small)
	for i := 1; i < len(files); i++ {
		key := files[i]
		j := i - 1
		for j >= 0 && compareBytes(files[j].MinKey, key.MinKey) > 0 {
			files[j+1] = files[j]
			j--
		}
		files[j+1] = key
	}
}

// compareBytes compares two byte slices.
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}
