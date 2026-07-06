package service

import "github.com/tuxi/flux-workflow/engine"

// 以下服务是引擎能力，实现已迁移至 engine 包；
// 这里保留类型别名，业务侧（handler/server）无需感知迁移。

// 任务重试（engine/task_retry.go）
type TaskRetryService = engine.TaskRetryService

type RetryTrigger = engine.RetryTrigger

const (
	RetryTriggerManual   = engine.RetryTriggerManual
	RetryTriggerRecovery = engine.RetryTriggerRecovery
	TaskNoRetryFound     = engine.TaskNoRetryFound
)

var NewTaskRetryService = engine.NewTaskRetryService

// redo / fork（engine/redo.go, engine/task_fork.go）
type RunRedoService = engine.RunRedoService

type TaskForkService = engine.TaskForkService

var NewTaskForkService = engine.NewTaskForkService

// 节点回放（engine/node_replay.go）
type NodeReplayService = engine.NodeReplayService

type NodeReplayResult = engine.NodeReplayResult

var NewNodeReplayService = engine.NewNodeReplayService

// 任务取消（engine/run_cancellation.go）
type RunCancellationService = engine.RunCancellationService

type RunCancellationResult = engine.RunCancellationResult

var NewRunCancellationService = engine.NewRunCancellationService

const CancelReasonSupersededByRevision = engine.CancelReasonSupersededByRevision

var (
	ErrRunCancellationTaskNotFound = engine.ErrRunCancellationTaskNotFound
	ErrRunCancellationNotAllowed   = engine.ErrRunCancellationNotAllowed
)
