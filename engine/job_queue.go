package engine

import (
	"context"
	"errors"
)

// ErrNoJob 表示当前没有可消费的异步任务（空轮询）。
// 队列实现（如 Redis Stream 的 block 超时）应返回该哨兵错误，
// 消费方据此静默重试，无需感知底层驱动的具体错误类型。
var ErrNoJob = errors.New("async job queue: no job available")

type AsyncJobQueue interface {

	// 发布任务
	Publish(ctx context.Context, job AsyncJob) error

	// Worker消费
	Consume(ctx context.Context, group string, consumer string) (*AsyncJob, string, error)

	// ack
	Ack(ctx context.Context, id string) error
}
