package catalog

import (
	"sync"
	"sync/atomic"
	"github.com/sfomuseum/go-lookup"
)

type SyncMapCatalog struct {
	lookup.Catalog
	catalog *sync.Map
}

func NewSyncMapCatalog() (lookup.Catalog, error) {

	catalog := new(sync.Map)

	m := SyncMapCatalog{
		catalog: catalog,
	}

	return &m, nil
}

func (m *SyncMapCatalog) Load(k string) (interface{}, bool) {
	return m.catalog.Load(k)
}

func (m *SyncMapCatalog) LoadOrStore(key string, value interface{}) (interface{}, bool) {
	return m.catalog.LoadOrStore(key, value)
}

func (m *SyncMapCatalog) Delete(key string) {
	m.catalog.Delete(key)
}

func (m *SyncMapCatalog) Range(f func(key, value interface{}) bool) error {
	m.catalog.Range(f)
	return nil
}

func (m *SyncMapCatalog) Count() int32 {

	remaining := int32(0)

	range_func := func(key, value interface{}) bool {
		atomic.AddInt32(&remaining, 1)
		return true
	}

	m.Range(range_func)
	return remaining
}