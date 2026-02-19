package importer

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ============================================================================
// Mock Engine
// ============================================================================

type mockEngine struct {
	data    map[string][]byte
	flushed int
}

func newMockEngine() *mockEngine {
	return &mockEngine{data: make(map[string][]byte)}
}

func (m *mockEngine) Put(key, value []byte) error {
	m.data[string(key)] = append([]byte{}, value...)
	return nil
}

func (m *mockEngine) Get(key []byte) ([]byte, error) {
	if v, ok := m.data[string(key)]; ok {
		return v, nil
	}
	return nil, nil
}

func (m *mockEngine) Delete(key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *mockEngine) Scan(start, end []byte) Iterator {
	return newMockIterator(m.data, start, end)
}

func (m *mockEngine) ForceFlush() error {
	m.flushed++
	return nil
}

type mockIterator struct {
	keys   []string
	values [][]byte
	pos    int
}

func newMockIterator(data map[string][]byte, start, end []byte) *mockIterator {
	var keys []string
	var values [][]byte

	for k, v := range data {
		if len(start) > 0 && k < string(start) {
			continue
		}
		if len(end) > 0 && k >= string(end) {
			continue
		}
		keys = append(keys, k)
		values = append(values, v)
	}

	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
				values[i], values[j] = values[j], values[i]
			}
		}
	}

	return &mockIterator{keys: keys, values: values}
}

func (m *mockIterator) Valid() bool   { return m.pos < len(m.keys) }
func (m *mockIterator) Key() []byte   { return []byte(m.keys[m.pos]) }
func (m *mockIterator) Value() []byte { return m.values[m.pos] }
func (m *mockIterator) Next()         { m.pos++ }
func (m *mockIterator) Close()        {}
func (m *mockIterator) Error() error  { return nil }

// ============================================================================
// CSV Import Tests
// ============================================================================

func TestImportCSV_Basic(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	csvData := `key1,value1
key2,value2
key3,value3`

	stats, err := imp.ImportCSVReader(context.Background(), strings.NewReader(csvData), CSVOptions{})
	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	if stats.RecordsImported != 3 {
		t.Errorf("Expected 3 records, got %d", stats.RecordsImported)
	}

	val, ok := engine.data["key1"]
	if !ok {
		t.Error("key1 not found")
	} else if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}
}

func TestImportCSV_WithHeader(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	csvData := `key,value
key1,value1
key2,value2`

	stats, err := imp.ImportCSVReader(context.Background(), strings.NewReader(csvData), CSVOptions{
		HasHeader: true,
	})
	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	if stats.RecordsImported != 2 {
		t.Errorf("Expected 2 records (header skipped), got %d", stats.RecordsImported)
	}
}

func TestImportCSV_CustomDelimiter(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	csvData := `key1;value1
key2;value2`

	stats, err := imp.ImportCSVReader(context.Background(), strings.NewReader(csvData), CSVOptions{
		Delimiter: ';',
	})
	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	if stats.RecordsImported != 2 {
		t.Errorf("Expected 2 records, got %d", stats.RecordsImported)
	}
}

func TestImportCSV_CustomColumns(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	csvData := `extra,key1,value1
extra,key2,value2`

	stats, err := imp.ImportCSVReader(context.Background(), strings.NewReader(csvData), CSVOptions{
		KeyColumn:   1,
		ValueColumn: 2,
	})
	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	if stats.RecordsImported != 2 {
		t.Errorf("Expected 2 records, got %d", stats.RecordsImported)
	}
}

func TestImportCSV_WithPrefix(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	csvData := `key1,value1`

	_, err := imp.ImportCSVReader(context.Background(), strings.NewReader(csvData), CSVOptions{
		KeyPrefix: "prefix:",
	})
	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	if _, ok := engine.data["prefix:key1"]; !ok {
		t.Error("Key prefix not applied")
	}
}

func TestImportCSV_SkipErrors(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	// Use column 0 and 1, "keyonly" has only column 0
	csvData := `key1,value1
keyonly
key2,value2`

	stats, err := imp.ImportCSVReader(context.Background(), strings.NewReader(csvData), CSVOptions{
		KeyColumn:   0,
		ValueColumn: 1,
		SkipErrors:  true,
	})
	if err != nil {
		t.Fatalf("Should not fail with SkipErrors: %v", err)
	}

	if stats.RecordsImported != 2 {
		t.Errorf("Expected 2 records, got %d", stats.RecordsImported)
	}

	if stats.RecordsFailed != 1 {
		t.Errorf("Expected 1 failure, got %d", stats.RecordsFailed)
	}
}

func TestImportCSV_File(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	dir := t.TempDir()
	path := filepath.Join(dir, "test.csv")
	os.WriteFile(path, []byte("key1,value1\nkey2,value2"), 0644)

	stats, err := imp.ImportCSV(context.Background(), path, CSVOptions{})
	if err != nil {
		t.Fatalf("ImportCSV failed: %v", err)
	}

	if stats.RecordsImported != 2 {
		t.Errorf("Expected 2 records, got %d", stats.RecordsImported)
	}
}

// ============================================================================
// JSON Import Tests
// ============================================================================

func TestImportJSON_Basic(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	jsonData := `{"key":"key1","value":"value1"}
{"key":"key2","value":"value2"}
{"key":"key3","value":"value3"}`

	stats, err := imp.ImportJSONReader(context.Background(), strings.NewReader(jsonData), JSONOptions{})
	if err != nil {
		t.Fatalf("ImportJSON failed: %v", err)
	}

	if stats.RecordsImported != 3 {
		t.Errorf("Expected 3 records, got %d", stats.RecordsImported)
	}
}

func TestImportJSON_CustomFields(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	jsonData := `{"k":"key1","v":"value1"}
{"k":"key2","v":"value2"}`

	stats, err := imp.ImportJSONReader(context.Background(), strings.NewReader(jsonData), JSONOptions{
		KeyField:   "k",
		ValueField: "v",
	})
	if err != nil {
		t.Fatalf("ImportJSON failed: %v", err)
	}

	if stats.RecordsImported != 2 {
		t.Errorf("Expected 2 records, got %d", stats.RecordsImported)
	}
}

func TestImportJSON_WithPrefix(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	jsonData := `{"key":"key1","value":"value1"}`

	_, err := imp.ImportJSONReader(context.Background(), strings.NewReader(jsonData), JSONOptions{
		KeyPrefix: "json:",
	})
	if err != nil {
		t.Fatalf("ImportJSON failed: %v", err)
	}

	if _, ok := engine.data["json:key1"]; !ok {
		t.Error("Key prefix not applied")
	}
}

func TestImportJSON_SkipErrors(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	jsonData := `{"key":"key1","value":"value1"}
invalid json
{"key":"key2","value":"value2"}`

	stats, err := imp.ImportJSONReader(context.Background(), strings.NewReader(jsonData), JSONOptions{
		SkipErrors: true,
	})
	if err != nil {
		t.Fatalf("Should not fail with SkipErrors: %v", err)
	}

	if stats.RecordsImported != 2 {
		t.Errorf("Expected 2 records, got %d", stats.RecordsImported)
	}
}

func TestImportJSON_File(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	dir := t.TempDir()
	path := filepath.Join(dir, "test.jsonl")
	os.WriteFile(path, []byte(`{"key":"k1","value":"v1"}`+"\n"+`{"key":"k2","value":"v2"}`), 0644)

	stats, err := imp.ImportJSON(context.Background(), path, JSONOptions{})
	if err != nil {
		t.Fatalf("ImportJSON failed: %v", err)
	}

	if stats.RecordsImported != 2 {
		t.Errorf("Expected 2 records, got %d", stats.RecordsImported)
	}
}

// ============================================================================
// CSV Export Tests
// ============================================================================

func TestExportCSV_Basic(t *testing.T) {
	engine := newMockEngine()
	engine.data["key1"] = []byte("value1")
	engine.data["key2"] = []byte("value2")

	imp := New(Options{Engine: engine})

	var buf bytes.Buffer
	stats, err := imp.ExportCSVWriter(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportCSV failed: %v", err)
	}

	if stats.RecordsExported != 2 {
		t.Errorf("Expected 2 records, got %d", stats.RecordsExported)
	}

	if !strings.Contains(buf.String(), "key1,value1") {
		t.Error("Missing key1")
	}
}

func TestExportCSV_WithHeader(t *testing.T) {
	engine := newMockEngine()
	engine.data["key1"] = []byte("value1")

	imp := New(Options{Engine: engine})

	var buf bytes.Buffer
	_, err := imp.ExportCSVWriter(context.Background(), &buf, ExportOptions{
		IncludeHeader: true,
	})
	if err != nil {
		t.Fatalf("ExportCSV failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if lines[0] != "key,value" {
		t.Errorf("Expected header, got '%s'", lines[0])
	}
}

func TestExportCSV_WithLimit(t *testing.T) {
	engine := newMockEngine()
	engine.data["key1"] = []byte("value1")
	engine.data["key2"] = []byte("value2")
	engine.data["key3"] = []byte("value3")

	imp := New(Options{Engine: engine})

	var buf bytes.Buffer
	stats, err := imp.ExportCSVWriter(context.Background(), &buf, ExportOptions{
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("ExportCSV failed: %v", err)
	}

	if stats.RecordsExported != 2 {
		t.Errorf("Expected 2 records (limit), got %d", stats.RecordsExported)
	}
}

func TestExportCSV_WithPrefix(t *testing.T) {
	engine := newMockEngine()
	engine.data["user:1"] = []byte("alice")
	engine.data["user:2"] = []byte("bob")
	engine.data["other:1"] = []byte("data")

	imp := New(Options{Engine: engine})

	var buf bytes.Buffer
	stats, err := imp.ExportCSVWriter(context.Background(), &buf, ExportOptions{
		KeyPrefix: "user:",
	})
	if err != nil {
		t.Fatalf("ExportCSV failed: %v", err)
	}

	if stats.RecordsExported != 2 {
		t.Errorf("Expected 2 records with prefix, got %d", stats.RecordsExported)
	}
}

func TestExportCSV_File(t *testing.T) {
	engine := newMockEngine()
	engine.data["key1"] = []byte("value1")

	imp := New(Options{Engine: engine})

	dir := t.TempDir()
	path := filepath.Join(dir, "export.csv")

	stats, err := imp.ExportCSV(context.Background(), path, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportCSV failed: %v", err)
	}

	if stats.RecordsExported != 1 {
		t.Errorf("Expected 1 record, got %d", stats.RecordsExported)
	}

	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "key1,value1") {
		t.Error("File content incorrect")
	}
}

// ============================================================================
// JSON Export Tests
// ============================================================================

func TestExportJSON_Basic(t *testing.T) {
	engine := newMockEngine()
	engine.data["key1"] = []byte("value1")
	engine.data["key2"] = []byte("value2")

	imp := New(Options{Engine: engine})

	var buf bytes.Buffer
	stats, err := imp.ExportJSONWriter(context.Background(), &buf, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportJSON failed: %v", err)
	}

	if stats.RecordsExported != 2 {
		t.Errorf("Expected 2 records, got %d", stats.RecordsExported)
	}

	if !strings.Contains(buf.String(), `"key":"key1"`) {
		t.Error("Missing key1")
	}
}

func TestExportJSON_WithLimit(t *testing.T) {
	engine := newMockEngine()
	engine.data["key1"] = []byte("value1")
	engine.data["key2"] = []byte("value2")
	engine.data["key3"] = []byte("value3")

	imp := New(Options{Engine: engine})

	var buf bytes.Buffer
	stats, err := imp.ExportJSONWriter(context.Background(), &buf, ExportOptions{
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("ExportJSON failed: %v", err)
	}

	if stats.RecordsExported != 1 {
		t.Errorf("Expected 1 record, got %d", stats.RecordsExported)
	}
}

func TestExportJSON_File(t *testing.T) {
	engine := newMockEngine()
	engine.data["key1"] = []byte("value1")

	imp := New(Options{Engine: engine})

	dir := t.TempDir()
	path := filepath.Join(dir, "export.jsonl")

	stats, err := imp.ExportJSON(context.Background(), path, ExportOptions{})
	if err != nil {
		t.Fatalf("ExportJSON failed: %v", err)
	}

	if stats.RecordsExported != 1 {
		t.Errorf("Expected 1 record, got %d", stats.RecordsExported)
	}
}

// ============================================================================
// Streaming Import Tests
// ============================================================================

func TestStreamImporter_Basic(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	stream := imp.NewStreamImporter(10)

	for i := 0; i < 25; i++ {
		key := []byte("key" + string(rune('0'+i%10)))
		value := []byte("value")
		if err := stream.Write(key, value); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	if err := stream.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	stats := stream.Stats()
	if stats.RecordsImported != 25 {
		t.Errorf("Expected 25 records, got %d", stats.RecordsImported)
	}

	if engine.flushed < 2 {
		t.Errorf("Expected at least 2 flushes, got %d", engine.flushed)
	}
}

// ============================================================================
// SSTable Builder Tests
// ============================================================================

func TestSSTableBuilder_Basic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	builder, err := NewSSTableBuilder(path, SSTableBuilderOptions{})
	if err != nil {
		t.Fatalf("NewSSTableBuilder failed: %v", err)
	}

	builder.Add([]byte("aaa"), []byte("value1"))
	builder.Add([]byte("bbb"), []byte("value2"))
	builder.Add([]byte("ccc"), []byte("value3"))

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	builder.Close()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("File not created: %v", err)
	}

	if info.Size() == 0 {
		t.Error("File should not be empty")
	}

	stats := builder.Stats()
	if stats.EntryCount != 3 {
		t.Errorf("Expected 3 entries, got %d", stats.EntryCount)
	}

	if stats.MinKey != "aaa" {
		t.Errorf("Expected min key 'aaa', got '%s'", stats.MinKey)
	}

	if stats.MaxKey != "ccc" {
		t.Errorf("Expected max key 'ccc', got '%s'", stats.MaxKey)
	}
}

// ============================================================================
// SSTable Importer Tests
// ============================================================================

func TestSSTableImporter_ImportFile(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	os.MkdirAll(dataDir, 0755)

	sstPath := filepath.Join(dir, "test.sst")
	os.WriteFile(sstPath, []byte("fake sstable data"), 0644)

	importer := NewSSTableImporter(SSTableImportOptions{
		DataDir: dataDir,
	})

	stats, err := importer.ImportSSTable(context.Background(), sstPath)
	if err != nil {
		t.Fatalf("ImportSSTable failed: %v", err)
	}

	if stats.FilesImported != 1 {
		t.Errorf("Expected 1 file imported, got %d", stats.FilesImported)
	}

	entries, _ := os.ReadDir(filepath.Join(dataDir, "sst"))
	if len(entries) != 1 {
		t.Errorf("Expected 1 file in sst dir, got %d", len(entries))
	}
}

func TestSSTableImporter_ImportDirectory(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	sourceDir := filepath.Join(dir, "source")
	os.MkdirAll(dataDir, 0755)
	os.MkdirAll(sourceDir, 0755)

	os.WriteFile(filepath.Join(sourceDir, "001.sst"), []byte("data1"), 0644)
	os.WriteFile(filepath.Join(sourceDir, "002.sst"), []byte("data2"), 0644)
	os.WriteFile(filepath.Join(sourceDir, "skip.txt"), []byte("skip"), 0644)

	importer := NewSSTableImporter(SSTableImportOptions{
		DataDir: dataDir,
	})

	stats, err := importer.ImportDirectory(context.Background(), sourceDir)
	if err != nil {
		t.Fatalf("ImportDirectory failed: %v", err)
	}

	if stats.FilesImported != 2 {
		t.Errorf("Expected 2 files imported, got %d", stats.FilesImported)
	}
}

// ============================================================================
// Error Cases
// ============================================================================

func TestImport_NoEngine(t *testing.T) {
	imp := New(Options{})

	_, err := imp.ImportCSVReader(context.Background(), strings.NewReader("a,b"), CSVOptions{})
	if err == nil {
		t.Error("Expected error with no engine")
	}
}

func TestImport_Concurrent(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	imp.inProgress.Store(true)

	_, err := imp.ImportCSVReader(context.Background(), strings.NewReader("a,b"), CSVOptions{})
	if err == nil {
		t.Error("Expected error for concurrent import")
	}
}

func TestExport_NoEngine(t *testing.T) {
	imp := New(Options{})

	var buf bytes.Buffer
	_, err := imp.ExportCSVWriter(context.Background(), &buf, ExportOptions{})
	if err == nil {
		t.Error("Expected error with no engine")
	}
}

// ============================================================================
// Stats Tests
// ============================================================================

func TestImporter_Stats(t *testing.T) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	csvData := "key1,value1"
	imp.ImportCSVReader(context.Background(), strings.NewReader(csvData), CSVOptions{})

	stats := imp.Stats()
	if stats.TotalImports != 1 {
		t.Errorf("Expected 1 total import, got %d", stats.TotalImports)
	}
}

// ============================================================================
// Helper Tests
// ============================================================================

func TestPrefixEnd(t *testing.T) {
	tests := []struct {
		prefix   string
		expected string
	}{
		{"abc", "abd"},
		{"a", "b"},
		{"", ""},
		{"\xff", ""},
	}

	for _, tt := range tests {
		result := prefixEnd([]byte(tt.prefix))
		if string(result) != tt.expected {
			t.Errorf("prefixEnd(%q) = %q, expected %q", tt.prefix, result, tt.expected)
		}
	}
}

// ============================================================================
// Benchmarks
// ============================================================================

func BenchmarkImportCSV(b *testing.B) {
	engine := newMockEngine()
	imp := New(Options{Engine: engine})

	var sb strings.Builder
	for i := 0; i < 10000; i++ {
		sb.WriteString("key")
		sb.WriteString(string(rune('0' + i%10)))
		sb.WriteString(",value\n")
	}
	data := sb.String()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		imp.inProgress.Store(false)
		imp.ImportCSVReader(context.Background(), strings.NewReader(data), CSVOptions{})
	}
}

func BenchmarkExportJSON(b *testing.B) {
	engine := newMockEngine()
	for i := 0; i < 1000; i++ {
		engine.data["key"+string(rune('0'+i%10))] = []byte("value")
	}

	imp := New(Options{Engine: engine})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		imp.inProgress.Store(false)
		var buf bytes.Buffer
		imp.ExportJSONWriter(context.Background(), &buf, ExportOptions{})
	}
}
