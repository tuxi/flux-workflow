package engine

import (
	"github.com/tuxi/flux-workflow/cost"
	"github.com/tuxi/flux-workflow/repository"
	"github.com/tuxi/flux-workflow/workflow"

	"github.com/tuxi/flux-workflow/eventbus"
	"github.com/tuxi/flux-workflow/pkg/lock"
)

// EngineOption is a functional option for configuring an Engine.
type EngineOption func(*engineConfig)

type engineConfig struct {
	taskRepo                  repository.TaskRepository
	nodeRepo                  repository.NodeRuntimeRepository
	awaitBindingRepo          repository.AwaitBindingRepository
	workflowVersionRepo       repository.WorkflowVersionRepository
	workflowRepo              repository.WorkflowRepository
	builder                   *workflow.Builder
	eventBus                  *eventbus.EventBus
	eventRepo                 repository.EventRepository
	jobQueue                  AsyncJobQueue
	dLocker                   lock.DistributedLock
	costRecorder              cost.Recorder
	subWorkflowBindingEnabled bool
}

// WithTaskRepo is required — stores and retrieves task state.
func WithTaskRepo(r repository.TaskRepository) EngineOption {
	return func(c *engineConfig) { c.taskRepo = r }
}

func WithSubWorkflowBinding(enabled bool) EngineOption {
	return func(c *engineConfig) { c.subWorkflowBindingEnabled = enabled }
}

// WithNodeRepo is required — stores and retrieves per-node runtime state.
func WithNodeRepo(r repository.NodeRuntimeRepository) EngineOption {
	return func(c *engineConfig) { c.nodeRepo = r }
}

// WithWorkflowVersionRepo is required — loads workflow definitions from storage.
func WithWorkflowVersionRepo(r repository.WorkflowVersionRepository) EngineOption {
	return func(c *engineConfig) { c.workflowVersionRepo = r }
}

// WithWorkflowRepo is required — loads workflow metadata.
func WithWorkflowRepo(r repository.WorkflowRepository) EngineOption {
	return func(c *engineConfig) { c.workflowRepo = r }
}

// WithBuilder is required — compiles WorkflowDefinition into executable Workflow.
func WithBuilder(b *workflow.Builder) EngineOption {
	return func(c *engineConfig) { c.builder = b }
}

// WithEventBus is required — publishes task/node events.
func WithEventBus(b *eventbus.EventBus) EngineOption {
	return func(c *engineConfig) { c.eventBus = b }
}

// WithEventRepo is required — persists task events.
func WithEventRepo(r repository.EventRepository) EngineOption {
	return func(c *engineConfig) { c.eventRepo = r }
}

// WithJobQueue is required — enables async node execution and sub-workflow scheduling.
func WithJobQueue(q AsyncJobQueue) EngineOption {
	return func(c *engineConfig) { c.jobQueue = q }
}

// WithAwaitBindingRepo enables await/signal nodes.
func WithAwaitBindingRepo(r repository.AwaitBindingRepository) EngineOption {
	return func(c *engineConfig) { c.awaitBindingRepo = r }
}

// WithDistributedLock enables sub-workflow coordination across workers.
func WithDistributedLock(l lock.DistributedLock) EngineOption {
	return func(c *engineConfig) { c.dLocker = l }
}

// WithCostRecorder sets a custom cost recorder (default: noop).
func WithCostRecorder(r cost.Recorder) EngineOption {
	return func(c *engineConfig) { c.costRecorder = r }
}

// New creates an Engine from functional options.
//
// The options-based constructor is the preferred way to create an Engine.
// The legacy NewEngine(a,b,c,...) positional constructor remains for
// backward compatibility.
func New(opts ...EngineOption) *Engine {
	cfg := &engineConfig{}
	for _, o := range opts {
		o(cfg)
	}

	eng := NewEngine(
		cfg.taskRepo,
		cfg.nodeRepo,
		cfg.awaitBindingRepo,
		cfg.workflowVersionRepo,
		cfg.workflowRepo,
		cfg.builder,
		cfg.eventBus,
		cfg.jobQueue,
		cfg.dLocker,
		cfg.eventRepo,
	)
	if cfg.costRecorder != nil {
		eng.SetCostRecorder(cfg.costRecorder)
	}
	eng.SetSubWorkflowBinding(cfg.subWorkflowBindingEnabled)
	return eng
}
