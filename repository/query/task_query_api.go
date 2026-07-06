package query

import (
	"context"

	"flux-workflow/domain"
	"flux-workflow/dto"
	"flux-workflow/repository"
)

// TaskQueryRepository 在核心 repository.TaskRepository 之上扩展面向业务
// （HTTP/API）的分页列表与详情查询。这些方法返回展示层 dto 类型，因此与
// 引擎运行时无关，独立于核心接口，随业务侧一同演进/迁移。
//
// 具体实现由 *taskRepository 一并提供，通过 NewTaskRepository 构造。
type TaskQueryRepository interface {
	repository.TaskRepository

	// ListByUser 分页列出某用户的任务。
	ListByUser(ctx context.Context, userID int64, params dto.PageRequest) ([]*domain.Task, int64, error)

	// ListByUserV2 轻量列表查询。
	ListByUserV2(ctx context.Context, userID int64, req dto.TaskListReq) ([]*dto.Task, int64, error)

	// GetTaskDetail 取任务详情。
	GetTaskDetail(ctx context.Context, taskID int64) (*dto.TaskDetail, error)
}
