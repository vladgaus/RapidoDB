package admin

import (
	"context"

	"github.com/vladgaus/RapidoDB/pkg/importer"
)

// ImporterAdapter adapts importer.Importer to the admin.ImportExportManager interface.
type ImporterAdapter struct {
	imp *importer.Importer
}

// NewImporterAdapter creates an adapter that implements ImportExportManager.
func NewImporterAdapter(imp *importer.Importer) *ImporterAdapter {
	return &ImporterAdapter{imp: imp}
}

// ImportCSV implements ImportExportManager.ImportCSV.
func (a *ImporterAdapter) ImportCSV(ctx context.Context, path string, opts ImportCSVOptions) (*ImportStats, error) {
	delimiter := ','
	if opts.Delimiter != "" && len(opts.Delimiter) > 0 {
		delimiter = rune(opts.Delimiter[0])
	}

	stats, err := a.imp.ImportCSV(ctx, path, importer.CSVOptions{
		HasHeader:   opts.HasHeader,
		KeyColumn:   opts.KeyColumn,
		ValueColumn: opts.ValueColumn,
		Delimiter:   delimiter,
		KeyPrefix:   opts.KeyPrefix,
		SkipErrors:  opts.SkipErrors,
	})
	if err != nil {
		return nil, err
	}

	return &ImportStats{
		RecordsTotal:     stats.RecordsTotal,
		RecordsImported:  stats.RecordsImported,
		RecordsFailed:    stats.RecordsFailed,
		BytesWritten:     stats.BytesWritten,
		DurationMs:       stats.Duration.Milliseconds(),
		RecordsPerSecond: stats.RecordsPerSecond,
	}, nil
}

// ImportJSON implements ImportExportManager.ImportJSON.
func (a *ImporterAdapter) ImportJSON(ctx context.Context, path string, opts ImportJSONOptions) (*ImportStats, error) {
	stats, err := a.imp.ImportJSON(ctx, path, importer.JSONOptions{
		KeyField:   opts.KeyField,
		ValueField: opts.ValueField,
		KeyPrefix:  opts.KeyPrefix,
		SkipErrors: opts.SkipErrors,
	})
	if err != nil {
		return nil, err
	}

	return &ImportStats{
		RecordsTotal:     stats.RecordsTotal,
		RecordsImported:  stats.RecordsImported,
		RecordsFailed:    stats.RecordsFailed,
		BytesWritten:     stats.BytesWritten,
		DurationMs:       stats.Duration.Milliseconds(),
		RecordsPerSecond: stats.RecordsPerSecond,
	}, nil
}

// ExportCSV implements ImportExportManager.ExportCSV.
func (a *ImporterAdapter) ExportCSV(ctx context.Context, path string, opts ExportOptions) (*ExportStats, error) {
	stats, err := a.imp.ExportCSV(ctx, path, importer.ExportOptions{
		StartKey:      opts.StartKey,
		EndKey:        opts.EndKey,
		KeyPrefix:     opts.KeyPrefix,
		Limit:         opts.Limit,
		IncludeHeader: opts.IncludeHeader,
	})
	if err != nil {
		return nil, err
	}

	return &ExportStats{
		RecordsExported:  stats.RecordsExported,
		BytesWritten:     stats.BytesWritten,
		DurationMs:       stats.Duration.Milliseconds(),
		RecordsPerSecond: stats.RecordsPerSecond,
	}, nil
}

// ExportJSON implements ImportExportManager.ExportJSON.
func (a *ImporterAdapter) ExportJSON(ctx context.Context, path string, opts ExportOptions) (*ExportStats, error) {
	stats, err := a.imp.ExportJSON(ctx, path, importer.ExportOptions{
		StartKey:  opts.StartKey,
		EndKey:    opts.EndKey,
		KeyPrefix: opts.KeyPrefix,
		Limit:     opts.Limit,
	})
	if err != nil {
		return nil, err
	}

	return &ExportStats{
		RecordsExported:  stats.RecordsExported,
		BytesWritten:     stats.BytesWritten,
		DurationMs:       stats.Duration.Milliseconds(),
		RecordsPerSecond: stats.RecordsPerSecond,
	}, nil
}

// Stats implements ImportExportManager.Stats.
func (a *ImporterAdapter) Stats() ImportExportStats {
	stats := a.imp.Stats()
	return ImportExportStats{
		TotalImports: stats.TotalImports,
		TotalExports: stats.TotalExports,
		InProgress:   stats.InProgress,
	}
}
