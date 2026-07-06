package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/tuxi/flux-workflow/domain"

	"github.com/stretchr/testify/require"
	"github.com/tuxi/flux/definition"
	"github.com/tuxi/flux/tool"
)

func helloDef() *definition.WorkflowDefinition {
	return &definition.WorkflowDefinition{
		Name: "hello_flow",
		Desc: "minimal start→end workflow for runtime tests",
		Nodes: []definition.NodeDefinition{
			{Name: "start", Type: definition.NodeStart},
			{Name: "end", Type: definition.NodeEnd},
		},
		Edges: []definition.EdgeDefinition{
			{From: "start", To: "end", Type: definition.EdgeNormal},
		},
	}
}

// awaitDef 构造 start → wait_event(await/signal) → end 的最小挂起工作流。
func awaitDef() *definition.WorkflowDefinition {
	return &definition.WorkflowDefinition{
		Name: "await_flow",
		Nodes: []definition.NodeDefinition{
			{Name: "start", Type: definition.NodeStart},
			{Name: "wait_event", Type: definition.NodeAwait, Config: map[string]any{
				"await_type": "signal",
				"source":     "signal",
			}},
			{Name: "end", Type: definition.NodeEnd},
		},
		Edges: []definition.EdgeDefinition{
			{From: "start", To: "wait_event", Type: definition.EdgeNormal},
			{From: "wait_event", To: "end", Type: definition.EdgeNormal},
		},
	}
}

// flakyTool 首次调用失败、之后成功，用于验证 Retry 闭环。
type flakyTool struct {
	mu    sync.Mutex
	calls int
}

func (f *flakyTool) Name() string                  { return "flaky_tool" }
func (f *flakyTool) Description() string           { return "fails on first call, then succeeds" }
func (f *flakyTool) InputSchema() tool.DataSchema  { return tool.DataSchema{} }
func (f *flakyTool) OutputSchema() tool.DataSchema { return tool.DataSchema{} }
func (f *flakyTool) Mode() tool.ExecutionMode      { return tool.SyncExecution }

func (f *flakyTool) Execute(_ context.Context, _ map[string]any, _ tool.ToolEmitter) (*tool.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls == 1 {
		return nil, fmt.Errorf("flaky: first attempt fails")
	}
	return tool.Success(map[string]any{"ok": true}), nil
}

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	r, err := NewLocal(filepath.Join(t.TempDir(), "state.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown() })
	return r
}

func TestRegisterWorkflow_PersistsVersion(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()
	def := helloDef()

	require.NoError(t, r.RegisterWorkflow(ctx, def))

	ver, err := r.wfVerRepo.GetLatestByWorkflowName(ctx, def.Name)
	require.NoError(t, err)
	require.NotNil(t, ver)
	require.Equal(t, def.Hash(), ver.Hash)

	var stored definition.WorkflowDefinition
	require.NoError(t, json.Unmarshal(ver.DefinitionJSON, &stored))
	require.Equal(t, def.Name, stored.Name)
	require.Len(t, stored.Nodes, 2)

	// 同名重复注册返回错误而非 panic
	require.Error(t, r.RegisterWorkflow(ctx, helloDef()))
}

func TestSubmit_BindsTaskToVersionAndEnqueues(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()
	def := helloDef()
	require.NoError(t, r.RegisterWorkflow(ctx, def))

	ver, err := r.wfVerRepo.GetLatestByWorkflowName(ctx, def.Name)
	require.NoError(t, err)

	taskID, err := r.Submit(ctx, def.Name, map[string]any{"topic": "AI"})
	require.NoError(t, err)
	require.NotZero(t, taskID)

	task, err := r.Status(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, domain.TaskPending, task.Status)
	require.Equal(t, ver.ID, task.WorkflowVersionID)
	require.Equal(t, ver.WorkflowID, task.WorkflowDefinitionID)

	popCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	popped, err := r.queue.PopAndReserve(popCtx)
	require.NoError(t, err)
	require.Equal(t, taskID, popped)
}

func TestSubmit_UnknownWorkflowFails(t *testing.T) {
	r := newTestRuntime(t)

	_, err := r.Submit(context.Background(), "no_such_flow", nil)
	require.Error(t, err)

	_, err = r.Submit(context.Background(), "", nil)
	require.Error(t, err)
}

// TestSubmit_TaskExecutableByWorker 模拟 worker.handle 的消费路径：
// 出队 → 按 task.WorkflowVersionID 加载定义 → engine 执行。
// 证明 Submit 产生的任务无需额外上下文即可被执行。
func TestSubmit_TaskExecutableByWorker(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()
	def := helloDef()
	require.NoError(t, r.RegisterWorkflow(ctx, def))

	taskID, err := r.Submit(ctx, def.Name, map[string]any{"topic": "AI"})
	require.NoError(t, err)

	popCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	popped, err := r.queue.PopAndReserve(popCtx)
	require.NoError(t, err)

	task, err := r.eng.TaskRepo().GetByID(ctx, popped)
	require.NoError(t, err)

	ver, err := r.wfVerRepo.Get(ctx, task.WorkflowVersionID)
	require.NoError(t, err)

	var loaded definition.WorkflowDefinition
	require.NoError(t, json.Unmarshal(ver.DefinitionJSON, &loaded))

	result := r.eng.RunWithResult(ctx, task, &loaded)
	require.NoError(t, result.Err)

	final, err := r.Status(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, domain.TaskSuccess, final.Status)
}

// TestStart_ExecutesSubmittedTasks 端到端：Start 拉起后台 worker 后，
// Submit 的任务无需手动驱动即被消费执行至终态。
func TestStart_ExecutesSubmittedTasks(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()
	def := helloDef()
	require.NoError(t, r.RegisterWorkflow(ctx, def))

	require.NoError(t, r.Start(ctx, WithTaskWorkers(1), WithAsyncWorkers(1)))

	// 重复 Start 返回错误
	require.Error(t, r.Start(ctx))

	taskID, err := r.Submit(ctx, def.Name, map[string]any{"topic": "AI"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		task, err := r.Status(ctx, taskID)
		return err == nil && task != nil && task.Status == domain.TaskSuccess
	}, 10*time.Second, 50*time.Millisecond, "submitted task should be executed by background workers")
}

// TestShutdown_StopsWorkers 验证 Shutdown 能让所有 worker goroutine 退出而不挂死。
func TestShutdown_StopsWorkers(t *testing.T) {
	r, err := NewLocal(filepath.Join(t.TempDir(), "state.db"))
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background()))

	done := make(chan struct{})
	go func() {
		_ = r.Shutdown()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not complete within 5s")
	}
}

// TestResume_WakesSuspendedAwaitTask 验证挂起→唤醒闭环：
// await 节点挂起任务后，Resume 携带节点输出闭合节点并同步续跑到 success。
func TestResume_WakesSuspendedAwaitTask(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()
	def := awaitDef()
	require.NoError(t, r.RegisterWorkflow(ctx, def))

	res, err := r.Run(ctx, def, map[string]any{"topic": "AI"})
	require.NoError(t, err)
	require.Equal(t, "suspended", res.Status)

	task, err := r.Status(ctx, res.TaskID)
	require.NoError(t, err)
	require.Equal(t, domain.TaskSuspended, task.Status)

	rr, err := r.Resume(ctx, res.TaskID, "wait_event", map[string]any{"approved": true})
	require.NoError(t, err)
	require.NoError(t, rr.Err)
	require.Equal(t, "success", rr.Status)
	require.Equal(t, domain.TaskSuccess, rr.Task.Status)

	// 幂等：节点已处理，重复唤醒返回 noop
	rr2, err := r.Resume(ctx, res.TaskID, "wait_event", map[string]any{"approved": true})
	require.NoError(t, err)
	require.Equal(t, "noop", rr2.Status)
}

// TestRetry_RerunsFailedTask 验证人工恢复闭环：任务失败终态后
// Retry 重置失败子树并重新入队，由后台 worker 重跑至 success。
func TestRetry_RerunsFailedTask(t *testing.T) {
	r, err := NewLocal(filepath.Join(t.TempDir(), "state.db"), WithLocalTool(&flakyTool{}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Shutdown() })
	ctx := context.Background()

	def := &definition.WorkflowDefinition{
		Name: "flaky_flow",
		Nodes: []definition.NodeDefinition{
			{Name: "start", Type: definition.NodeStart},
			// retry_count: 0 关闭节点级重试，让首次失败直接成为任务失败
			{Name: "work", Type: definition.NodeTool, Config: map[string]any{"tool": "flaky_tool", "retry_count": 0}},
			{Name: "end", Type: definition.NodeEnd},
		},
		Edges: []definition.EdgeDefinition{
			{From: "start", To: "work", Type: definition.EdgeNormal},
			{From: "work", To: "end", Type: definition.EdgeNormal},
		},
	}
	require.NoError(t, r.RegisterWorkflow(ctx, def))
	require.NoError(t, r.Start(ctx, WithTaskWorkers(1), WithAsyncWorkers(1)))

	taskID, err := r.Submit(ctx, def.Name, nil)
	require.NoError(t, err)

	// 首次执行：工具失败，自动重试耗尽后任务落 failed
	require.Eventually(t, func() bool {
		task, err := r.Status(ctx, taskID)
		return err == nil && task != nil && task.Status == domain.TaskFailed
	}, 10*time.Second, 50*time.Millisecond, "task should fail on first tool attempt")

	// Retry 前置校验：pending/running 之外才允许（此时是 failed，应成功）
	require.NoError(t, r.Retry(ctx, taskID, "", nil))

	require.Eventually(t, func() bool {
		task, err := r.Status(ctx, taskID)
		return err == nil && task != nil && task.Status == domain.TaskSuccess
	}, 10*time.Second, 50*time.Millisecond, "retried task should succeed on second tool attempt")

	// 成功终态后不允许再 Retry
	require.Error(t, r.Retry(ctx, taskID, "", nil))
}

func TestRun_StampsVersionWhenRegistered(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()
	def := helloDef()
	require.NoError(t, r.RegisterWorkflow(ctx, def))

	ver, err := r.wfVerRepo.GetLatestByWorkflowName(ctx, def.Name)
	require.NoError(t, err)

	res, err := r.Run(ctx, def, map[string]any{"topic": "AI"})
	require.NoError(t, err)
	require.NoError(t, res.Err)
	require.Equal(t, "success", res.Status)
	require.NotNil(t, res.Task)
	require.Equal(t, ver.ID, res.Task.WorkflowVersionID)
}
