package importer

import (
	"github.com/vladgaus/RapidoDB/pkg/lsm"
)

// LSMEngineWrapper wraps an LSM engine to implement the importer.Engine interface.
type LSMEngineWrapper struct {
	engine *lsm.Engine
}

// NewLSMEngineWrapper creates a wrapper around an LSM engine.
func NewLSMEngineWrapper(engine *lsm.Engine) *LSMEngineWrapper {
	return &LSMEngineWrapper{engine: engine}
}

// Put implements Engine.Put.
func (w *LSMEngineWrapper) Put(key, value []byte) error {
	return w.engine.Put(key, value)
}

// Get implements Engine.Get.
func (w *LSMEngineWrapper) Get(key []byte) ([]byte, error) {
	return w.engine.Get(key)
}

// Delete implements Engine.Delete.
func (w *LSMEngineWrapper) Delete(key []byte) error {
	return w.engine.Delete(key)
}

// Scan implements Engine.Scan, returning an Iterator interface.
func (w *LSMEngineWrapper) Scan(start, end []byte) Iterator {
	return w.engine.Scan(start, end)
}

// ForceFlush implements Engine.ForceFlush.
func (w *LSMEngineWrapper) ForceFlush() error {
	return w.engine.ForceFlush()
}
