//go:build windows

package health

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// DiskChecker checks disk space health.
type DiskChecker struct {
	// Path is the directory to check.
	Path string

	// WarningThresholdPercent triggers degraded status when disk is this full.
	WarningThresholdPercent float64

	// CriticalThresholdPercent triggers unhealthy status.
	CriticalThresholdPercent float64

	// MinFreeBytes is the minimum free space required.
	MinFreeBytes uint64
}

// NewDiskChecker creates a disk checker for the given path.
func NewDiskChecker(path string) *DiskChecker {
	return &DiskChecker{
		Path:                     path,
		WarningThresholdPercent:  80.0,
		CriticalThresholdPercent: 95.0,
		MinFreeBytes:             100 * 1024 * 1024, // 100MB minimum
	}
}

// Name returns the checker name.
func (c *DiskChecker) Name() string {
	return "disk"
}

// Check performs the disk health check.
func (c *DiskChecker) Check() CheckResult {
	start := time.Now()

	// Check if path exists
	if _, err := os.Stat(c.Path); os.IsNotExist(err) {
		return CheckResult{
			Name:      c.Name(),
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("Path does not exist: %s", c.Path),
			Duration:  time.Since(start),
			Timestamp: time.Now(),
		}
	}

	// Get disk stats using Windows API
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	getDiskFreeSpaceEx := kernel32.NewProc("GetDiskFreeSpaceExW")

	var freeBytesAvailable, totalBytes, totalFreeBytes uint64

	pathPtr, err := syscall.UTF16PtrFromString(c.Path)
	if err != nil {
		return CheckResult{
			Name:      c.Name(),
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("Invalid path: %v", err),
			Duration:  time.Since(start),
			Timestamp: time.Now(),
		}
	}

	ret, _, err := getDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&totalBytes)),
		uintptr(unsafe.Pointer(&totalFreeBytes)),
	)

	if ret == 0 {
		return CheckResult{
			Name:      c.Name(),
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("Failed to get disk stats: %v", err),
			Duration:  time.Since(start),
			Timestamp: time.Now(),
		}
	}

	// Calculate sizes
	usedBytes := totalBytes - totalFreeBytes
	usedPercent := float64(usedBytes) / float64(totalBytes) * 100

	status := StatusHealthy
	message := fmt.Sprintf("%.1f%% used (%.1f GB free)",
		usedPercent, float64(freeBytesAvailable)/1024/1024/1024)

	// Check thresholds
	if usedPercent >= c.CriticalThresholdPercent {
		status = StatusUnhealthy
		message = fmt.Sprintf("Critical: %.1f%% used (%.1f GB free)",
			usedPercent, float64(freeBytesAvailable)/1024/1024/1024)
	} else if usedPercent >= c.WarningThresholdPercent {
		status = StatusDegraded
		message = fmt.Sprintf("Warning: %.1f%% used (%.1f GB free)",
			usedPercent, float64(freeBytesAvailable)/1024/1024/1024)
	}

	// Check minimum free space
	if freeBytesAvailable < c.MinFreeBytes {
		status = StatusUnhealthy
		message = fmt.Sprintf("Insufficient space: %.1f MB free (need %.1f MB)",
			float64(freeBytesAvailable)/1024/1024, float64(c.MinFreeBytes)/1024/1024)
	}

	return CheckResult{
		Name:      c.Name(),
		Status:    status,
		Message:   message,
		Duration:  time.Since(start),
		Timestamp: time.Now(),
		Details: map[string]interface{}{
			"path":         c.Path,
			"total_bytes":  totalBytes,
			"used_bytes":   usedBytes,
			"free_bytes":   totalFreeBytes,
			"avail_bytes":  freeBytesAvailable,
			"used_percent": usedPercent,
		},
	}
}
