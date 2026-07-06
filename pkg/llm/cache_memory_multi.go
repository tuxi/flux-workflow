package llm

import (
	"math/rand"
	"sync"
	"time"
)

type MemoryMultiCache struct {
	mu         sync.RWMutex
	items      map[string][]cacheItem
	defaultTTL int
	maxItems   int
}

func NewMemoryMultiCache(defaultTTL int, maxItem int) *MemoryMultiCache {
	return &MemoryMultiCache{
		items:      make(map[string][]cacheItem),
		defaultTTL: defaultTTL,
		maxItems:   maxItem,
	}
}

// -----------------------------------------------------------------------------
// 【最终版】GetRandom：内部自动判断是否攒满，未满直接返回 false
// -----------------------------------------------------------------------------
func (m *MemoryMultiCache) GetRandom(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	list, ok := m.items[key]
	if !ok || len(list) == 0 {
		return "", false
	}

	now := time.Now().Unix()
	var validItems []cacheItem

	for _, item := range list {
		if now-item.Timestamp < int64(item.TTL) {
			validItems = append(validItems, item)
		}
	}

	if len(validItems) == 0 {
		return "", false
	}

	// ==============================
	// 核心：没攒满 → 不返回缓存
	// ==============================
	if len(validItems) < m.maxItems {
		return "", false
	}

	idx := rand.Intn(len(validItems))
	return validItems[idx].Data, true
}

func (m *MemoryMultiCache) Append(key string, value string) {
	m.AppendWithTTL(key, value, m.defaultTTL)
}

func (m *MemoryMultiCache) AppendWithTTL(key string, value string, seconds int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	item := cacheItem{
		Data:      value,
		Timestamp: time.Now().Unix(),
		TTL:       seconds,
	}

	list := m.items[key]
	list = append(list, item)

	if len(list) > m.maxItems {
		list = list[len(list)-m.maxItems:]
	}

	m.items[key] = list
}
