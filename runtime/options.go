package runtime

import (
	"github.com/tuxi/flux-workflow/cost"
	"github.com/tuxi/flux-workflow/engine"
	"github.com/tuxi/flux-workflow/eventbus"
	"github.com/tuxi/flux-workflow/pkg/lock"
	"github.com/tuxi/flux-workflow/repository"

	"github.com/tuxi/flux/tool"
	"gorm.io/gorm"
)

// Option configures a server-side Runtime.
type Option func(*Options)

// Options holds all externally-provided dependencies.
type Options struct {
	DB    *gorm.DB
	Queue repository.TaskQueue
	Lock  lock.DistributedLock
	Bus   *eventbus.EventBus
	JobQ  engine.AsyncJobQueue

	tools     []toolItem
	costRec   cost.Recorder
	awaitRepo repository.AwaitBindingRepository
}

type toolItem struct {
	t    tool.Tool
	name string
}

// LocalOption configures a local (single-process) Runtime.
type LocalOption func(*localOptions)

type localOptions struct {
	tools []toolItem
}

// WithDB sets the GORM database handle.
func WithDB(db *gorm.DB) Option { return func(o *Options) { o.DB = db } }

// WithQueue sets the task queue.
func WithQueue(q repository.TaskQueue) Option { return func(o *Options) { o.Queue = q } }

// WithLock sets the distributed lock.
func WithLock(l lock.DistributedLock) Option { return func(o *Options) { o.Lock = l } }

// WithEventBus sets the event bus.
func WithEventBus(b *eventbus.EventBus) Option { return func(o *Options) { o.Bus = b } }

// WithJobQueue sets the async job queue.
func WithJobQueue(q engine.AsyncJobQueue) Option { return func(o *Options) { o.JobQ = q } }

// WithTool registers a tool for use in tool-type workflow nodes.
func WithTool(t tool.Tool) Option {
	return func(o *Options) { o.tools = append(o.tools, toolItem{t: t}) }
}

// WithCostRecorder sets a custom cost recorder.
func WithCostRecorder(r cost.Recorder) Option { return func(o *Options) { o.costRec = r } }

// WithAwaitBindingRepo sets the await binding repository (enables await nodes).
func WithAwaitBindingRepo(r repository.AwaitBindingRepository) Option {
	return func(o *Options) { o.awaitRepo = r }
}

// WithLocalTool registers a tool for local mode.
func WithLocalTool(t tool.Tool) LocalOption {
	return func(o *localOptions) { o.tools = append(o.tools, toolItem{t: t}) }
}
