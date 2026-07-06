// Package redislock provides a Redsync (Redis) backed lock.DistributedLock.
//
// It is an optional driver kept out of pkg/lock so that local/in-memory
// embedding does not pull in redsync / go-redis.
package redislock

import (
	"context"
	"fmt"
	"time"

	"github.com/tuxi/flux-workflow/pkg/lock"

	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
)

// RedisLock is a Redsync-backed distributed lock (recommended for production).
type RedisLock struct {
	rs *redsync.Redsync
}

var _ lock.DistributedLock = (*RedisLock)(nil)

// New builds a Redis-backed distributed lock over the given client.
func New(redisClient *redis.Client) *RedisLock {
	pool := goredis.NewPool(redisClient)
	return &RedisLock{
		rs: redsync.New(pool),
	}
}

// Lock 抢占分布式锁
func (l *RedisLock) Lock(ctx context.Context, key string, timeout time.Duration) (bool, func(), error) {
	mutex := l.rs.NewMutex(
		key,
		redsync.WithExpiry(timeout),
		redsync.WithTries(100), // 增加到 100 次
		redsync.WithRetryDelay(100*time.Millisecond), // 间隔缩小，反应更快
	)
	// 总等待能力 = 100 * 100ms = 10 秒

	// 抢占锁
	// 此时 mutex.LockContext 会阻塞直到拿到锁或重试耗尽
	if err := mutex.LockContext(ctx); err != nil {
		return false, nil, fmt.Errorf("lock failed: %w", err)
	}

	// 解锁函数
	unlock := func() {
		_, _ = mutex.UnlockContext(ctx)
	}

	return true, unlock, nil
}
