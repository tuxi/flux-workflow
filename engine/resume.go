package engine

import (
	"context"
	"flux-workflow/domain"
	"flux-workflow/workflow"
	"fmt"
	"time"
)

// ResumeTask 恢复 Workflow 任务执行
func (e *Engine) ResumeTask(
	taskID int64,
	nodeName string,
	meta map[string]any,
) RunResult {
	// --- 阶段一：抢锁阶段 ---
	// 设置一个专门用于抢锁排队的上下文，1 分钟
	lockCtx, lockCancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer lockCancel()

	lockKey := fmt.Sprintf("resume_task:%d", taskID)

	// 内部会阻塞重试10次获取锁，这对于任务调度的微妙不确定时机很有用
	start := time.Now()
	locked, unlock, err := e.dLocker.Lock(lockCtx, lockKey, 2*time.Minute)
	if waitTime := time.Since(start); waitTime > time.Second {
		fmt.Printf("⚠️ 锁竞争激烈: lockKey=%s, 等待了=%v\n", lockKey, waitTime)
	}
	fmt.Println("Engine.ResumeTask:抢占锁, lockKey:", lockKey)
	if err != nil {
		// 如果获取锁2分钟内还是没有取到锁，则把任务放到队列中等待重新调度
		go func() { // 延迟几秒加入队列，确保下次能跑
			time.Sleep(3 * time.Second)
			e.requeuePendingEdgesResume(context.Background(), taskID, nodeName)
		}()
		fmt.Println("Engine.ResumeTask:抢占锁失败, lockKey:，error:", lockKey, err.Error())
		return RunResult{Status: RunNoop}
	}
	defer func() {
		if unlock != nil {
			fmt.Println("Engine.ResumeTask:释放锁, lockKey:", lockKey)
			unlock()
		}
	}()

	if !locked {
		return RunResult{Status: RunNoop}
	}

	// --- 阶段二：业务执行阶段 ---
	// 抢到锁后，我们需要一个长期有效的上下文。
	// 使用 Background，意味着由业务逻辑自己控制什么时候结束。
	taskCtx := context.Background()

	// 1. 拿当前节点 runtime
	nodeRuntime, err := e.nodeRepo.FindByTaskIDAndNode(taskCtx, taskID, nodeName)
	if err != nil {
		return RunResult{
			Status: RunFailed,
			Err:    fmt.Errorf("check node status error: %w", err),
		}
	}
	if nodeRuntime == nil {
		return RunResult{
			Status: RunFailed,
			Err:    fmt.Errorf("node runtime not found: task=%d node=%s", taskID, nodeName),
		}
	}

	// 2. 如果节点已经终态，直接 noop
	if nodeRuntime.State == domain.NodeSuccess ||
		nodeRuntime.State == domain.NodeFailed ||
		nodeRuntime.State == domain.NodeSkipped ||
		nodeRuntime.State == domain.NodeCanceled {
		fmt.Printf("Task %d Node %s already processed (state: %s), skip\n", taskID, nodeName, nodeRuntime.State)
		return RunResult{Status: RunNoop}
	}

	// 3. 任务状态检查
	task, err := e.taskRepo.GetByID(taskCtx, taskID)
	if err != nil {
		return RunResult{
			Status: RunFailed,
			Err:    err,
		}
	}
	if task == nil {
		return RunResult{
			Status: RunFailed,
			Err:    fmt.Errorf("task not found: %d", taskID),
		}
	}
	if task.Status != domain.TaskSuspended && task.Status != domain.TaskRunning {
		return RunResult{
			Status: RunFailed,
			Err:    fmt.Errorf("task not resumable by engine resume, use prepareTaskRetry first, current status=%s", task.Status),
		}
	}

	// 4. 加载 workflow
	wf, _, err := e.loadWorkflowForTask(taskCtx, task)
	if err != nil {
		return RunResult{
			Status: RunFailed,
			Err:    err,
		}
	}

	// 5. 构建 runCtx
	runCtx := e.newRunContext(taskCtx, task, wf)

	// 6. 只恢复当前 task 自己的 runtime，不能重新做 fork planning/materialization
	if err := e.loadOrInitRuntime(runCtx, wf); err != nil {
		return RunResult{
			Status: RunFailed,
			Err:    err,
		}
	}

	// 7. 恢复 activated edges
	e.rebuildActivatedEdges(runCtx)

	// 8. 将当前 async/subworkflow 完成节点的 public output 补回 ctx.Output
	publicOutput, err := e.buildResumePublicOutput(wf, nodeName, meta, nodeRuntime)
	if err != nil {
		return RunResult{
			Status: RunFailed,
			Err:    err,
		}
	}

	node := findNode(wf.Nodes(), nodeName)
	if node == nil {
		return RunResult{
			Status: RunFailed,
			Err:    fmt.Errorf("node %s not found", nodeName),
		}
	}

	if len(publicOutput) > 0 {
		if err := runCtx.SetNodeOutput(
			nodeName,
			publicOutput,
			node.Step.OutputSchema(),
		); err != nil {
			return RunResult{
				Status: RunFailed,
				Err:    err,
			}
		}
	}

	// 9. 同步一下 runtime 内存态，确保 runDAG/finalize 看的是最新值
	if rt := runCtx.Runtime[nodeName]; rt != nil && len(publicOutput) > 0 {
		rt.Output = deepCloneMap(publicOutput)
		rt.OutputHash = runCtx.CalculateOutputHash(publicOutput)
		if usageFacts := extractAwaitUsageFacts(meta); len(usageFacts) > 0 {
			if rt.Checkpoint == nil {
				rt.Checkpoint = map[string]any{}
			}
			rt.Checkpoint["usage_facts"] = usageFacts
		}
	}

	// 10. 继续执行，不重复发 task_started
	result := e.executeTask(runCtx, wf, false)
	if result.Status == RunFailed {
		e.publishResumeFinalFailed(task, result.Err, nodeName)
	}
	return result
}

// CompleteNodeAndResume 外部事件唤醒挂起任务的统一入口：
// 用 meta 闭合 nodeName 节点（写输出、状态转 success-pending-edges），随后继续执行 DAG。
//
// 若该节点存在等待中的 await binding（await 节点挂起），走 CompleteAwaitNode
// 保证 binding 状态一并闭合；否则走 async 节点的抢占式闭合 + ResumeTask。
// 节点已被其他线程处理时返回 RunNoop，可安全重复调用。
func (e *Engine) CompleteNodeAndResume(
	taskID int64,
	nodeName string,
	meta map[string]any,
	errMsg string,
) RunResult {
	if e.awaitBindingRepo != nil {
		binding, err := e.awaitBindingRepo.GetByTaskAndNode(context.Background(), taskID, nodeName)
		if err == nil && binding != nil && binding.Status == domain.AwaitBindingWaiting {
			return e.CompleteAwaitNode(binding.ID, meta, errMsg, "engine.complete_node_and_resume")
		}
	}

	ok, err := e.completeAsyncNode(taskID, nodeName, meta, errMsg)
	if err != nil {
		return RunResult{Status: RunFailed, Err: err}
	}
	if !ok {
		return RunResult{Status: RunNoop}
	}
	return e.ResumeTask(taskID, nodeName, meta)
}

func (e *Engine) requeuePendingEdgesResume(ctx context.Context, taskID int64, nodeName string) {
	if e == nil || e.taskRepo == nil || e.nodeRepo == nil {
		return
	}

	runtime, err := e.nodeRepo.FindByTaskIDAndNode(ctx, taskID, nodeName)
	if err != nil || runtime == nil {
		return
	}
	switch runtime.State {
	case domain.NodeSuccessPendingEdges, domain.NodeFailedPendingEdges:
	default:
		return
	}

	task, err := e.taskRepo.GetByID(ctx, taskID)
	if err != nil || task == nil {
		return
	}
	switch task.Status {
	case domain.TaskSuspended:
		task.Status = domain.TaskPending
		task.WorkerID = ""
		task.StartedAt = time.Time{}
		if err := e.taskRepo.Update(ctx, task); err != nil {
			return
		}
	case domain.TaskPending, domain.TaskRunning:
	default:
		return
	}

	_ = e.taskRepo.Enqueue(ctx, taskID)
}

func extractAwaitUsageFacts(meta map[string]any) []map[string]any {
	if len(meta) == 0 {
		return nil
	}
	return toUsageFacts(meta[AwaitUsageFactsMetaKey])
}

func (e *Engine) buildResumePublicOutput(
	wf workflow.Workflow,
	nodeName string,
	meta map[string]any,
	runtime *domain.NodeRuntime,
) (map[string]any, error) {
	nodeMap := wf.Nodes()
	node := findNode(nodeMap, nodeName)
	if node == nil {
		return nil, fmt.Errorf("node %s not found", nodeName)
	}

	if len(meta) > 0 {
		publicOutput, _ := splitAwaitEventPayload(meta)
		return deepCloneMap(publicOutput), nil
	}
	if runtime != nil && runtime.Output != nil {
		return deepCloneMap(runtime.Output), nil
	}
	return map[string]any{}, nil
}

func (e *Engine) publishResumeFinalFailed(task *domain.Task, err error, nodeName string) {
	if e == nil || e.eventBus == nil || task == nil {
		return
	}

	reason := "resume execution failed"
	if err != nil && err.Error() != "" {
		reason = err.Error()
	}

	e.eventBus.Publish(task.RootID, &domain.TaskEvent{
		TaskID:     task.ID,
		RootTaskID: task.RootID,
		Step:       "task",
		Type:       domain.TaskEventFinalFailed,
		Message:    "任务最终失败",
		Error:      reason,
		Meta: map[string]any{
			"final_reason":      "resume_failed",
			"error_message":     reason,
			"retry_count":       task.RetryCount,
			"retry_limit":       0,
			"retry_exhausted":   false,
			"source":            "engine.resume_task",
			"resume_node":       nodeName,
			"last_run_status":   string(domain.TaskFailed),
			"can_manual_resume": true,
		},
		CreatedAt: time.Now(),
	})
}
