package llm

// -------------------------
// MultiCache 多结果缓存
type MultiCache interface {
	// 获取一个随机结果, 缓存未满 → 返回 false；缓存满 → 随机返回
	GetRandom(key string) (string, bool)
	// 追加一个结果（自动限制最大数量）
	Append(key string, value string)
	// 追加带TTL
	AppendWithTTL(key string, value string, seconds int)
}

// 缓存条目结构（内部使用）
type cacheItem struct {
	Data      string `json:"d"`
	Timestamp int64  `json:"ts"` // 存入时间
	TTL       int    `json:"ttl"`
}
