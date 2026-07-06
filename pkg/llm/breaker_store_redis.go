package llm

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisBreakerStore struct {
	client *redis.Client
	ctx    context.Context
	prefix string
}

func NewRedisBreakerStore(client *redis.Client, prefix string) *RedisBreakerStore {
	if prefix == "" {
		prefix = "llm:breaker"
	}
	return &RedisBreakerStore{
		client: client,
		ctx:    context.Background(),
		prefix: prefix,
	}
}

func (s *RedisBreakerStore) Load(key string) (breakerState, bool, error) {
	if s == nil || s.client == nil {
		return breakerState{}, false, nil
	}

	raw, err := s.client.Get(s.ctx, s.redisKey(key)).Result()
	if err == redis.Nil {
		return breakerState{}, false, nil
	}
	if err != nil {
		return breakerState{}, false, err
	}

	var state breakerState
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return breakerState{}, false, err
	}
	return state, true, nil
}

func (s *RedisBreakerStore) Save(key string, state breakerState, ttl time.Duration) error {
	if s == nil || s.client == nil {
		return nil
	}

	bs, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	return s.client.Set(s.ctx, s.redisKey(key), bs, ttl).Err()
}

func (s *RedisBreakerStore) Delete(key string) error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Del(s.ctx, s.redisKey(key)).Err()
}

func (s *RedisBreakerStore) redisKey(key string) string {
	return s.prefix + ":" + key
}
