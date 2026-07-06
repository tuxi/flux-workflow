package llm

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisMultiCache 支持：一个key存多个结果 + 独立TTL + 随机读取
type RedisMultiCache struct {
	client     *redis.Client
	ctx        context.Context
	defaultTTL int
	maxItems   int
}

func NewRedisMultiCache(client *redis.Client, defaultTTL int, maxItems int) *RedisMultiCache {
	return &RedisMultiCache{
		client:     client,
		ctx:        context.Background(),
		defaultTTL: defaultTTL,
		maxItems:   maxItems,
	}
}

// GetRandom 内部自动判断是否攒满，未满直接返回 false
func (r *RedisMultiCache) GetRandom(key string) (string, bool) {
	list, err := r.client.LRange(r.ctx, key, 0, -1).Result()
	if err != nil {
		log.Printf("llm redis cache get random failed: redis_key=%s err=%v", key, err)
		return "", false
	}
	if len(list) == 0 {
		log.Printf("llm redis cache miss: redis_key=%s list_len=0 reason=empty_list", key)
		return "", false
	}

	now := time.Now().Unix()
	var validItems []cacheItem

	for _, s := range list {
		var item cacheItem
		if err := json.Unmarshal([]byte(s), &item); err != nil {
			log.Printf("llm redis cache item decode failed: redis_key=%s err=%v", key, err)
			continue
		}
		if now-item.Timestamp < int64(item.TTL) {
			validItems = append(validItems, item)
		}
	}

	// 清理无效数据
	if len(validItems) == 0 {
		_ = r.client.Del(r.ctx, key).Err()
		log.Printf("llm redis cache miss: redis_key=%s list_len=%d valid_len=0 reason=all_items_expired", key, len(list))
		return "", false
	}

	// ==============================
	// 没攒满 → 不返回缓存
	// ==============================
	if len(validItems) < r.maxItems {
		log.Printf("llm redis cache miss: redis_key=%s list_len=%d valid_len=%d max_items=%d reason=not_enough_items", key, len(list), len(validItems), r.maxItems)
		return "", false
	}

	// 攒满了 → 随机返回
	idx := rand.Intn(len(validItems))
	log.Printf("llm redis cache hit: redis_key=%s list_len=%d valid_len=%d max_items=%d selected_idx=%d", key, len(list), len(validItems), r.maxItems, idx)
	return validItems[idx].Data, true
}

// Append 追加（默认TTL
func (r *RedisMultiCache) Append(key string, value string) {
	r.AppendWithTTL(key, value, r.defaultTTL)
}

// AppendWithTTL 追加一个结果，并限制最大条数
func (r *RedisMultiCache) AppendWithTTL(key string, value string, seconds int) {
	item := cacheItem{
		Data:      value,
		Timestamp: time.Now().Unix(),
		TTL:       seconds,
	}
	bs, _ := json.Marshal(item)

	pipe := r.client.Pipeline()
	pipe.RPush(r.ctx, key, bs)
	pipe.LTrim(r.ctx, key, -int64(r.maxItems), -1) // 保留最新N条
	pipe.Expire(r.ctx, key, time.Hour*24*7)        // 列表本身7天过期
	if _, err := pipe.Exec(r.ctx); err != nil {
		log.Printf("llm redis cache append failed: redis_key=%s ttl_seconds=%d max_items=%d err=%v", key, seconds, r.maxItems, err)
		return
	}

	listLen, err := r.client.LLen(r.ctx, key).Result()
	if err != nil {
		log.Printf("llm redis cache append success but llen failed: redis_key=%s ttl_seconds=%d max_items=%d err=%v", key, seconds, r.maxItems, err)
		return
	}

	log.Printf("llm redis cache append success: redis_key=%s list_len=%d ttl_seconds=%d max_items=%d", key, listLen, seconds, r.maxItems)
}
