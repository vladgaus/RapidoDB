package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ============================================================================
// Backend Interface
// ============================================================================

// Backend is the interface for backup storage.
type Backend interface {
	// Write writes data from a reader to the given path.
	Write(ctx context.Context, path string, reader io.Reader) error

	// WriteBytes writes bytes to the given path.
	WriteBytes(ctx context.Context, path string, data []byte) error

	// Read returns a reader for the given path.
	Read(ctx context.Context, path string) (io.ReadCloser, error)

	// Delete deletes the given path (file or directory).
	Delete(ctx context.Context, path string) error

	// List lists entries in the given directory.
	List(ctx context.Context, prefix string) ([]string, error)

	// Exists checks if a path exists.
	Exists(ctx context.Context, path string) (bool, error)

	// MkdirAll creates directories.
	MkdirAll(ctx context.Context, path string) error

	// Type returns the backend type name.
	Type() string

	// Close closes the backend.
	Close() error
}

// ============================================================================
// Local Backend
// ============================================================================

// LocalBackend stores backups on local filesystem.
type LocalBackend struct {
	basePath string
}

// NewLocalBackend creates a new local filesystem backend.
func NewLocalBackend(basePath string) *LocalBackend {
	return &LocalBackend{basePath: basePath}
}

// Write writes data to a local file.
func (b *LocalBackend) Write(ctx context.Context, path string, reader io.Reader) error {
	fullPath := filepath.Join(b.basePath, path)

	// Create directory
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	// Create file
	f, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, reader)
	return err
}

// WriteBytes writes bytes to a local file.
func (b *LocalBackend) WriteBytes(ctx context.Context, path string, data []byte) error {
	fullPath := filepath.Join(b.basePath, path)

	// Create directory
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	return os.WriteFile(fullPath, data, 0644)
}

// Read returns a reader for a local file.
func (b *LocalBackend) Read(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := filepath.Join(b.basePath, path)
	return os.Open(fullPath)
}

// Delete deletes a local file or directory.
func (b *LocalBackend) Delete(ctx context.Context, path string) error {
	fullPath := filepath.Join(b.basePath, path)
	return os.RemoveAll(fullPath)
}

// List lists entries in a local directory.
func (b *LocalBackend) List(ctx context.Context, prefix string) ([]string, error) {
	fullPath := filepath.Join(b.basePath, prefix)

	entries, err := os.ReadDir(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}

	return names, nil
}

// Exists checks if a local path exists.
func (b *LocalBackend) Exists(ctx context.Context, path string) (bool, error) {
	fullPath := filepath.Join(b.basePath, path)
	_, err := os.Stat(fullPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// MkdirAll creates local directories.
func (b *LocalBackend) MkdirAll(ctx context.Context, path string) error {
	fullPath := filepath.Join(b.basePath, path)
	return os.MkdirAll(fullPath, 0755)
}

// Type returns "local".
func (b *LocalBackend) Type() string {
	return "local"
}

// Close closes the backend (no-op for local).
func (b *LocalBackend) Close() error {
	return nil
}

// BasePath returns the base path.
func (b *LocalBackend) BasePath() string {
	return b.basePath
}

// ============================================================================
// S3 Backend
// ============================================================================

// S3Backend stores backups in S3-compatible storage.
// This is a minimal implementation - for production use,
// you would want to use the AWS SDK.
type S3Backend struct {
	bucket    string
	prefix    string
	endpoint  string
	region    string
	accessKey string
	secretKey string
}

// S3Options configures the S3 backend.
type S3Options struct {
	// Bucket is the S3 bucket name.
	Bucket string

	// Prefix is an optional path prefix within the bucket.
	Prefix string

	// Endpoint is the S3 endpoint (for S3-compatible services).
	// Leave empty for AWS S3.
	Endpoint string

	// Region is the AWS region.
	Region string

	// AccessKey is the AWS access key ID.
	AccessKey string

	// SecretKey is the AWS secret access key.
	SecretKey string
}

// NewS3Backend creates a new S3 backend.
// Note: This is a stub implementation. For production use,
// integrate with the AWS SDK or a compatible library.
func NewS3Backend(opts S3Options) (*S3Backend, error) {
	if opts.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}

	return &S3Backend{
		bucket:    opts.Bucket,
		prefix:    opts.Prefix,
		endpoint:  opts.Endpoint,
		region:    opts.Region,
		accessKey: opts.AccessKey,
		secretKey: opts.SecretKey,
	}, nil
}

func (b *S3Backend) fullPath(path string) string {
	if b.prefix != "" {
		return filepath.Join(b.prefix, path)
	}
	return path
}

// Write writes data to S3.
func (b *S3Backend) Write(ctx context.Context, path string, reader io.Reader) error {
	// TODO: Implement using AWS SDK
	// For now, return an error indicating SDK is needed
	return fmt.Errorf("S3 backend requires AWS SDK integration - use local backend or implement AWS SDK calls")
}

// WriteBytes writes bytes to S3.
func (b *S3Backend) WriteBytes(ctx context.Context, path string, data []byte) error {
	return fmt.Errorf("S3 backend requires AWS SDK integration")
}

// Read reads from S3.
func (b *S3Backend) Read(ctx context.Context, path string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("S3 backend requires AWS SDK integration")
}

// Delete deletes from S3.
func (b *S3Backend) Delete(ctx context.Context, path string) error {
	return fmt.Errorf("S3 backend requires AWS SDK integration")
}

// List lists objects in S3.
func (b *S3Backend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, fmt.Errorf("S3 backend requires AWS SDK integration")
}

// Exists checks if an object exists in S3.
func (b *S3Backend) Exists(ctx context.Context, path string) (bool, error) {
	return false, fmt.Errorf("S3 backend requires AWS SDK integration")
}

// MkdirAll is a no-op for S3 (directories don't really exist).
func (b *S3Backend) MkdirAll(ctx context.Context, path string) error {
	return nil
}

// Type returns "s3".
func (b *S3Backend) Type() string {
	return "s3"
}

// Close closes the S3 backend.
func (b *S3Backend) Close() error {
	return nil
}

// Bucket returns the bucket name.
func (b *S3Backend) Bucket() string {
	return b.bucket
}

// ============================================================================
// Backend Factory
// ============================================================================

// ParseBackendURL parses a backup URL and returns the appropriate backend.
// Supported formats:
//   - /path/to/backups (local)
//   - file:///path/to/backups (local)
//   - s3://bucket/prefix (S3)
//   - gs://bucket/prefix (GCS - not implemented)
//   - az://container/prefix (Azure - not implemented)
func ParseBackendURL(url string, opts map[string]string) (Backend, error) {
	// Local path
	if strings.HasPrefix(url, "/") || strings.HasPrefix(url, "./") {
		return NewLocalBackend(url), nil
	}

	// file:// URL
	if strings.HasPrefix(url, "file://") {
		path := strings.TrimPrefix(url, "file://")
		return NewLocalBackend(path), nil
	}

	// s3:// URL
	if strings.HasPrefix(url, "s3://") {
		parts := strings.SplitN(strings.TrimPrefix(url, "s3://"), "/", 2)
		bucket := parts[0]
		prefix := ""
		if len(parts) > 1 {
			prefix = parts[1]
		}

		return NewS3Backend(S3Options{
			Bucket:    bucket,
			Prefix:    prefix,
			Endpoint:  opts["endpoint"],
			Region:    opts["region"],
			AccessKey: opts["access_key"],
			SecretKey: opts["secret_key"],
		})
	}

	// gs:// URL (GCS)
	if strings.HasPrefix(url, "gs://") {
		return nil, fmt.Errorf("GCS backend not implemented - use local backend")
	}

	// az:// URL (Azure)
	if strings.HasPrefix(url, "az://") {
		return nil, fmt.Errorf("Azure backend not implemented - use local backend")
	}

	// Assume local path
	return NewLocalBackend(url), nil
}
