package repository

import (
	"context"
	"flux-workflow/domain"
	"time"
)

// TaskRepository 是引擎运行时依赖的任务存储接口，只涉及 domain 类型，
// 不引入任何面向 HTTP/API 的展示层（dto）依赖。面向业务的分页/详情查询
// 见 repository/query.TaskQueryRepository。
type TaskRepository interface {
	Create(ctx context.Context, task *domain.Task) error
	GetByID(ctx context.Context, id int64) (*domain.Task, error)
	Update(ctx context.Context, task *domain.Task) error

	ListByParent(ctx context.Context, parentID int64) ([]*domain.Task, error)
	FindRunningRootTasks(ctx context.Context, before time.Time) ([]*domain.Task, error)

	FindByWorkflowID(ctx context.Context, workflowID int64) ([]*domain.Task, error)
	ListChildrenByParentID(ctx context.Context, parentID int64) ([]*domain.Task, error)

	// 批量更新更高效
	BatchUpdateStatus(ctx context.Context, taskIDs []int64, status domain.TaskStatus, errMsg string) error

	Enqueue(ctx context.Context, taskID int64) error
	// TryClaimTask 原子抢占任务（CAS）
	// 只允许一个 Worker 抢到任务
	// 同时支持 Running 超时任务重新抢占（Lease）
	TryClaimTask(ctx context.Context, taskID int64, workerID string) (bool, error)

	FindBySubKey(ctx context.Context, subKey string) (*domain.Task, error)
	ListByParentNode(ctx context.Context, parentID int64, nodeName string) ([]*domain.Task, error)

	CreateFork(ctx context.Context, source *domain.Task, newTaskID int64, newInput []byte, editAction, editLabel string) (*domain.Task, error)

	// 发布详情仍然取完整 task，但要求只能取 root task。返回 domain 类型，无 dto 依赖。
	GetRootTaskByIDAndUser(
		ctx context.Context,
		taskID int64,
		userID int64,
	) (*domain.Task, error)
}
