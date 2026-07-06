package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"flux-workflow/domain"
	"flux-workflow/engine"
	"flux-workflow/eventbus"
	"flux-workflow/pkg/lock"
	"flux-workflow/repository"
	"flux-workflow/repository/query"
	itime "flux-workflow/runtime/internal"
	"flux-workflow/workflow"
	"flux-workflow/workflow/nodes"

	"github.com/tuxi/flux/definition"
	"github.com/tuxi/flux/tool"
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
// After creation, Run executes a workflow definition synchronously.
type Runtime struct {
	eng     *engine.Engine
	bus     *eventbus.EventBus
	db      *gorm.DB
	queue   repository.TaskQueue
	nodeReg *nodes.NodeRegistry

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
	)

	return &Runtime{
		eng:     eng,
		bus:     bus,
		db:      db,
		queue:   queue,
		nodeReg: nodeReg,
		ownDB:   ownDB,
	}, nil
}

// Run executes a workflow definition synchronously.
func (r *Runtime) Run(
	ctx context.Context,
	def *definition.WorkflowDefinition,
	input map[string]any,
) (*RunResult, error) {
	taskID := genTaskID()
	task := &domain.Task{
		ID:     taskID,
		RootID: taskID,
		Status: domain.TaskPending,
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

// Submit enqueues a task for async execution. Returns the task ID.
func (r *Runtime) Submit(
	ctx context.Context,
	_ *definition.WorkflowDefinition,
	input map[string]any,
) (int64, error) {
	taskID := genTaskID()
	task := &domain.Task{
		ID:     taskID,
		RootID: taskID,
		Status: domain.TaskPending,
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

// Resume resumes a suspended task with a signal.
func (r *Runtime) Resume(ctx context.Context, taskID int64, signal domain.Signal) error {
	_ = taskID
	_ = signal
	_ = ctx
	return fmt.Errorf("runtime: Resume not yet implemented")
}

// Subscribe returns a channel that receives events for the given event type.
func (r *Runtime) Subscribe(eventType string) (<-chan *domain.TaskEvent, func()) {
	if r.bus == nil {
		ch := make(chan *domain.TaskEvent)
		close(ch)
		return ch, func() {}
	}
	return r.bus.Subscribe(eventType), func() {}
}

// Shutdown closes the database connection if owned by this Runtime.
func (r *Runtime) Shutdown() error {
	if r.ownDB && r.db != nil {
		if sqlDB, err := r.db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
	return nil
}

// Engine exposes the underlying engine for advanced use.
func (r *Runtime) Engine() *engine.Engine { return r.eng }

// NodeRegistry exposes the node registry for registering custom node types.
func (r *Runtime) NodeRegistry() *nodes.NodeRegistry { return r.nodeReg }

func genTaskID() int64 {
	return time.Now().UnixNano()
}

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
