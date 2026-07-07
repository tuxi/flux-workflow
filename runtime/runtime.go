package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/tuxi/flux-workflow/domain"
	"github.com/tuxi/flux-workflow/engine"
	"github.com/tuxi/flux-workflow/eventbus"
	"github.com/tuxi/flux-workflow/pkg/lock"
	"github.com/tuxi/flux-workflow/pkg/uuid"
	"github.com/tuxi/flux-workflow/registry"
	"github.com/tuxi/flux-workflow/repository"
	"github.com/tuxi/flux-workflow/repository/query"
	itime "github.com/tuxi/flux-workflow/runtime/internal"
	"github.com/tuxi/flux-workflow/worker"
	"github.com/tuxi/flux-workflow/workflow"
	"github.com/tuxi/flux-workflow/workflow/nodes"

	"github.com/tuxi/flux-workflow/definition"
	"github.com/tuxi/flux-workflow/tool"
	"gorm.io/gorm"
)

// Runtime is the one-stop entry point for the flux-workflow engine.
//
// Two constructors cover all use cases:
//
//	// Local mode — self-contained, SQLite, no external dependencies
//	r, _ := runtime.NewLocal("./state.db")
//
//	// Server mode — external DB, queue, lock, event bus
//	r, _ := runtime.New(
//	    runtime.WithDB(pgDB),
//	    runtime.WithQueue(redisQ),
//	    runtime.WithEventBus(bus),
//	    runtime.WithJobQueue(jobQ),
//	)
//
// After creation, Run executes a workflow definition synchronously, or
// Start launches background workers so Submit-ed tasks execute asynchronously.
type Runtime struct {
	eng       *engine.Engine
	bus       *eventbus.EventBus
	db        *gorm.DB
	queue     repository.TaskQueue
	nodeReg   *nodes.NodeRegistry
	wfReg     *registry.WorkflowRegistry
	wfVerRepo repository.WorkflowVersionRepository
	snow      *uuid.SnowNode

	// worker 构造所需依赖
	taskRepo  repository.TaskRepository
	nodeRepo  repository.NodeRuntimeRepository
	wfRepo    repository.WorkflowRepository
	awaitRepo repository.AwaitBindingRepository
	toolReg   *tool.Registry
	builder   *workflow.Builder
	dLock     lock.DistributedLock
	jobQ      engine.AsyncJobQueue
	retrySvc  engine.TaskRetryService

	// 生命周期
	mu      sync.Mutex
	started bool
	stop    context.CancelFunc
	wg      sync.WaitGroup

	ownDB bool
}

// RunResult represents the outcome of a Runtime.Run call.
type RunResult struct {
	TaskID        int64
	Status        string
	Err           error
	Task          *domain.Task
	SuspendReason string
	SuspendNode   string
}

// NewLocal creates a self-contained Runtime using SQLite.
//
// All dependencies are created internally:
//   - SQLite database (WAL mode, auto-migrate)
//   - In-memory task queue
//   - In-memory async job queue
//   - In-memory distributed lock
//   - Event bus (no-op persistence and WS, for local use)
//
// Tools can be registered via WithLocalTool.
func NewLocal(sqlitePath string, opts ...LocalOption) (*Runtime, error) {
	cfg := &localOptions{}
	for _, o := range opts {
		o(cfg)
	}

	db, err := openSQLite(sqlitePath)
	if err != nil {
		return nil, fmt.Errorf("runtime: %w", err)
	}

	memQ := itime.NewMemoryQueue(1024)
	memJobQ := itime.NewMemJobQueue(256)
	memLock := itime.NewMemoryLock()
	bus := eventbus.NewEventBus(nil, nil)

	return newRuntime(db, true, cfg.tools, memQ, memLock, memJobQ, bus)
}

// New creates a Runtime from externally-provided dependencies.
func New(opts ...Option) (*Runtime, error) {
	cfg := &Options{}
	for _, o := range opts {
		o(cfg)
	}

	if cfg.DB == nil {
		return nil, fmt.Errorf("runtime: DB is required, use WithDB")
	}
	if cfg.Queue == nil {
		return nil, fmt.Errorf("runtime: Queue is required, use WithQueue")
	}
	if cfg.Bus == nil {
		return nil, fmt.Errorf("runtime: EventBus is required, use WithEventBus")
	}
	if cfg.JobQ == nil {
		return nil, fmt.Errorf("runtime: JobQueue is required, use WithJobQueue")
	}

	return newRuntime(cfg.DB, false, cfg.tools, cfg.Queue, cfg.Lock, cfg.JobQ, cfg.Bus)
}

func newRuntime(
	db *gorm.DB,
	ownDB bool,
	tools []toolItem,
	queue repository.TaskQueue,
	dLock lock.DistributedLock,
	jobQ engine.AsyncJobQueue,
	bus *eventbus.EventBus,
) (*Runtime, error) {
	taskRepo := query.NewTaskRepository(db, queue)
	nodeRepo := query.NewNodeRuntimeRepository(db)
	wfRepo := query.NewWorkflowRepository(db)
	wfVerRepo := query.NewWorkflowVersionRepository(db)
	eventRepo := query.NewEventRepository(db)
	awaitRepo := query.NewAwaitBindingRepository(db)

	toolReg := tool.NewRegistry()
	for _, ti := range tools {
		toolReg.Register(ti.t)
	}

	nodeReg := nodes.NewNodeRegistry()
	nodes.RegisterBuiltinNodes(nodeReg)
	nodeReg.Register(nodes.NewToolFactory(toolReg), nodes.ToolNodeSchema)

	builder := workflow.NewBuilder(nodeReg)
	wfReg := registry.NewWorkflowRegistry(wfRepo, wfVerRepo)

	eng := engine.New(
		engine.WithTaskRepo(taskRepo),
		engine.WithNodeRepo(nodeRepo),
		engine.WithAwaitBindingRepo(awaitRepo),
		engine.WithWorkflowVersionRepo(wfVerRepo),
		engine.WithWorkflowRepo(wfRepo),
		engine.WithBuilder(builder),
		engine.WithEventBus(bus),
		engine.WithEventRepo(eventRepo),
		engine.WithJobQueue(jobQ),
		engine.WithDistributedLock(dLock),
		engine.WithSubWorkflowBinding(true),
	)
	retrySvc := engine.NewTaskRetryService(wfVerRepo, taskRepo, nodeRepo, awaitRepo, builder)

	return &Runtime{
		eng:       eng,
		bus:       bus,
		db:        db,
		queue:     queue,
		nodeReg:   nodeReg,
		wfReg:     wfReg,
		wfVerRepo: wfVerRepo,
		snow:      uuid.NewNode(0),
		taskRepo:  taskRepo,
		nodeRepo:  nodeRepo,
		wfRepo:    wfRepo,
		awaitRepo: awaitRepo,
		toolReg:   toolReg,
		builder:   builder,
		dLock:     dLock,
		jobQ:      jobQ,
		retrySvc:  retrySvc,
		ownDB:     ownDB,
	}, nil
}

// StartOptions 配置后台 worker 并发度。
type StartOptions struct {
	TaskWorkers      int // 任务执行 worker 数，默认 2
	AsyncWorkers     int // 异步节点 job worker 数，默认 2
	AwaitPollWorkers int // await 轮询 worker 数，默认 1
}

// StartOption configures Runtime.Start.
type StartOption func(*StartOptions)

// WithTaskWorkers sets the number of task execution workers.
func WithTaskWorkers(n int) StartOption { return func(o *StartOptions) { o.TaskWorkers = n } }

// WithAsyncWorkers sets the number of async node job workers.
func WithAsyncWorkers(n int) StartOption { return func(o *StartOptions) { o.AsyncWorkers = n } }

// WithAwaitPollWorkers sets the number of await poll workers.
func WithAwaitPollWorkers(n int) StartOption {
	return func(o *StartOptions) { o.AwaitPollWorkers = n }
}

// Start launches the background workers that consume Submit-ed tasks:
//
//   - task workers: 从任务队列取任务并交给 engine 执行
//   - recovery scanner: 扫描 crash 的 running 节点并自动恢复
//   - async workers: 消费异步节点 job 队列
//   - await poll workers: 轮询 await binding 的超时与到期
//
// Workers run until Shutdown is called or ctx is canceled.
// Start 只能调用一次；未调用 Start 时 Run 仍可同步执行工作流。
func (r *Runtime) Start(ctx context.Context, opts ...StartOption) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return fmt.Errorf("runtime: already started")
	}

	cfg := &StartOptions{TaskWorkers: 2, AsyncWorkers: 2, AwaitPollWorkers: 1}
	for _, o := range opts {
		o(cfg)
	}

	runCtx, cancel := context.WithCancel(ctx)
	r.stop = cancel
	r.started = true

	w := worker.NewWorker(
		r.eng,
		r.taskRepo,
		r.nodeRepo,
		r.wfVerRepo,
		r.wfRepo,
		r.queue,
		r.jobQ,
		r.bus,
		r.nodeReg,
		r.dLock,
		r.builder,
		r.retrySvc,
	)
	w.StartRecoveryScanner(runCtx)

	for i := 0; i < cfg.TaskWorkers; i++ {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			w.Loop(runCtx)
		}()
	}

	asyncW := worker.NewAsyncWorker(r.jobQ, r.taskRepo, r.nodeRepo, r.toolReg, r.bus)
	for i := 0; i < cfg.AsyncWorkers; i++ {
		consumer := fmt.Sprintf("async-%d", i)
		r.wg.Add(1)
		go func(c string) {
			defer r.wg.Done()
			asyncW.Start(runCtx, c)
		}(consumer)
	}

	pollW := worker.NewAwaitPollWorker(r.awaitRepo, r.toolReg, r.eng, r.bus, r.dLock)
	for i := 0; i < cfg.AwaitPollWorkers; i++ {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			pollW.Start(runCtx)
		}()
	}

	return nil
}

// RegisterWorkflow 注册工作流定义并立即持久化。
// 定义变更时自动发布新版本（按 def.Hash() 判定）；此后可通过
// Submit(ctx, def.Name, input) 按名称提交任务。
func (r *Runtime) RegisterWorkflow(ctx context.Context, def *definition.WorkflowDefinition) error {
	if def == nil || def.Name == "" {
		return fmt.Errorf("runtime: workflow definition must have a name")
	}
	return r.wfReg.RegisterAndSync(ctx, def)
}

// Run executes a workflow definition synchronously.
//
// The definition does not need to be registered. If it is registered and
// unchanged (same hash), the task is linked to the persisted version so it
// can later be resumed/replayed by version ID.
func (r *Runtime) Run(
	ctx context.Context,
	def *definition.WorkflowDefinition,
	input map[string]any,
) (*RunResult, error) {
	taskID := r.snow.GenSnowID()
	task := &domain.Task{
		ID:     taskID,
		RootID: taskID,
		Status: domain.TaskPending,
	}

	if def != nil && def.Name != "" {
		if ver, err := r.wfVerRepo.GetLatestByWorkflowName(ctx, def.Name); err == nil && ver != nil && ver.Hash == def.Hash() {
			task.WorkflowVersionID = ver.ID
			task.WorkflowDefinitionID = ver.WorkflowID
		}
	}

	if input != nil {
		b, err := json.Marshal(input)
		if err != nil {
			return nil, fmt.Errorf("runtime: marshal input: %w", err)
		}
		task.InputJSON = b
	}

	if err := r.eng.TaskRepo().Create(ctx, task); err != nil {
		return nil, fmt.Errorf("runtime: create task: %w", err)
	}

	engResult := r.eng.RunWithResult(ctx, task, def)

	updatedTask, _ := r.eng.TaskRepo().GetByID(ctx, taskID)

	return &RunResult{
		TaskID:        taskID,
		Status:        statusFromEngine(engResult.Status),
		Err:           engResult.Err,
		Task:          updatedTask,
		SuspendReason: engResult.SuspendReason,
		SuspendNode:   engResult.SuspendNode,
	}, nil
}

// Submit enqueues a task for async execution by workflow name and returns
// the task ID. The workflow must have been registered via RegisterWorkflow
// (or otherwise persisted); the task is bound to the latest version, which
// workers resolve to load the definition.
func (r *Runtime) Submit(
	ctx context.Context,
	workflowName string,
	input map[string]any,
) (int64, error) {
	if workflowName == "" {
		return 0, fmt.Errorf("runtime: workflow name is required")
	}

	ver, err := r.wfVerRepo.GetLatestByWorkflowName(ctx, workflowName)
	if err != nil || ver == nil {
		return 0, fmt.Errorf("runtime: workflow %q not registered: %w", workflowName, err)
	}

	taskID := r.snow.GenSnowID()
	task := &domain.Task{
		ID:                   taskID,
		RootID:               taskID,
		Status:               domain.TaskPending,
		WorkflowVersionID:    ver.ID,
		WorkflowDefinitionID: ver.WorkflowID,
	}

	if input != nil {
		b, err := json.Marshal(input)
		if err != nil {
			return 0, fmt.Errorf("runtime: marshal input: %w", err)
		}
		task.InputJSON = b
	}

	if err := r.eng.TaskRepo().Create(ctx, task); err != nil {
		return 0, fmt.Errorf("runtime: create task: %w", err)
	}
	if err := r.eng.TaskRepo().Enqueue(ctx, taskID); err != nil {
		return 0, fmt.Errorf("runtime: enqueue task: %w", err)
	}
	return taskID, nil
}

// Status returns the current task state.
func (r *Runtime) Status(ctx context.Context, taskID int64) (*domain.Task, error) {
	return r.eng.TaskRepo().GetByID(ctx, taskID)
}

// Resume 用外部事件的结果唤醒挂起中的任务：把 meta 作为 nodeName 节点的输出
// 闭合该节点，然后在当前调用内同步续跑 DAG，直到下一个终态或挂起点。
//
// 典型场景：await/async 节点挂起后，外部回调（webhook、人工审批、异步任务完成）
// 携带结果到达。节点已被处理时返回 Status "noop"，可安全重复调用。
// failed/canceled 的任务请使用 Retry。
func (r *Runtime) Resume(
	ctx context.Context,
	taskID int64,
	nodeName string,
	meta map[string]any,
) (*RunResult, error) {
	if taskID == 0 || nodeName == "" {
		return nil, fmt.Errorf("runtime: taskID and nodeName are required")
	}

	engResult := r.eng.CompleteNodeAndResume(taskID, nodeName, meta, "")

	updatedTask, _ := r.taskRepo.GetByID(ctx, taskID)

	return &RunResult{
		TaskID:        taskID,
		Status:        statusFromEngine(engResult.Status),
		Err:           engResult.Err,
		Task:          updatedTask,
		SuspendReason: engResult.SuspendReason,
		SuspendNode:   engResult.SuspendNode,
	}, nil
}

// Retry 人工恢复 failed / canceled / suspended 的任务：重置失败子树后重新入队，
// 由后台 worker（需已 Start）异步执行。
//
// resumeFrom 指定从某节点重跑（空则自动收集失败根节点）；patches 可在恢复前
// 修正上游节点的输出。手动重试会重置自动重试计数器。
func (r *Runtime) Retry(
	ctx context.Context,
	taskID int64,
	resumeFrom string,
	patches []domain.RuntimePatch,
) error {
	task, err := r.taskRepo.GetByID(ctx, taskID)
	if err != nil {
		return fmt.Errorf("runtime: load task: %w", err)
	}
	if task == nil {
		return fmt.Errorf("runtime: task not found: %d", taskID)
	}
	if task.Status != domain.TaskFailed &&
		task.Status != domain.TaskCanceled &&
		task.Status != domain.TaskSuspended {
		return fmt.Errorf("runtime: task %d is not retryable, status=%s", taskID, task.Status)
	}

	if err := r.retrySvc.PrepareTaskRetry(ctx, taskID, engine.RetryTriggerManual, resumeFrom, patches); err != nil {
		// 没有可重置的失败根节点（如任务在首个节点前就失败）：直接置回 pending 重新入队
		if err.Error() == engine.TaskNoRetryFound &&
			(task.Status == domain.TaskFailed || task.Status == domain.TaskCanceled) {
			task.Status = domain.TaskPending
			if err := r.taskRepo.Update(ctx, task); err != nil {
				return fmt.Errorf("runtime: reset task status: %w", err)
			}
			return r.taskRepo.Enqueue(ctx, taskID)
		}
		return fmt.Errorf("runtime: prepare retry: %w", err)
	}

	return r.taskRepo.Enqueue(ctx, taskID)
}

// Subscribe returns a channel that receives events for the given event type,
// plus a cancel function that unsubscribes and closes the channel.
func (r *Runtime) Subscribe(eventType string) (<-chan *domain.TaskEvent, func()) {
	if r.bus == nil {
		ch := make(chan *domain.TaskEvent)
		close(ch)
		return ch, func() {}
	}
	ch := r.bus.Subscribe(eventType)
	return ch, func() { r.bus.Unsubscribe(eventType, ch) }
}

// Shutdown stops background workers (if started), waits for them to exit,
// then closes the database connection if owned by this Runtime.
func (r *Runtime) Shutdown() error {
	r.mu.Lock()
	if r.stop != nil {
		r.stop()
		r.stop = nil
	}
	r.started = false
	r.mu.Unlock()

	r.wg.Wait()

	// 停掉 engine 的事件监听 goroutine，防止 DB 关闭后仍有监听器访问存储
	if r.eng != nil {
		r.eng.Close()
	}

	if r.ownDB && r.db != nil {
		if sqlDB, err := r.db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	return nil
}

// Engine exposes the underlying engine for advanced use (replay, redo, cancel,
// fork, and other power-user operations that are intentionally not on the facade).
func (r *Runtime) Engine() *engine.Engine { return r.eng }

// NodeRegistry exposes the node registry for registering custom node types.
func (r *Runtime) NodeRegistry() *nodes.NodeRegistry { return r.nodeReg }

// DB exposes the underlying *gorm.DB. With it a consumer can construct any
// query repository (via repository/query) or run its own read-side queries —
// the primary extension point for building a business/HTTP layer on top.
func (r *Runtime) DB() *gorm.DB { return r.db }

// EventBus exposes the event bus, for publishing custom events or attaching
// additional listeners beyond Subscribe.
func (r *Runtime) EventBus() *eventbus.EventBus { return r.bus }

// ToolRegistry exposes the tool registry the engine's tool nodes resolve
// against. Register additional tools on it (they resolve lazily at node
// execution) and share it with any service that executes tools directly, so
// the engine and the business layer see the same tool set.
func (r *Runtime) ToolRegistry() *tool.Registry { return r.toolReg }

func (r *Runtime) WorkflowRegistry() *registry.WorkflowRegistry { return r.wfReg }

func statusFromEngine(s engine.RunStatus) string {
	switch s {
	case engine.RunSuccess:
		return "success"
	case engine.RunSuspended:
		return "suspended"
	case engine.RunFailed:
		return "failed"
	case engine.RunNoop:
		return "noop"
	default:
		return string(s)
	}
}
