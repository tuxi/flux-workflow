package runtime_test

import (
	"path/filepath"
	"testing"

	"github.com/tuxi/flux-workflow/repository/query"
	"github.com/tuxi/flux-workflow/repository/query/taskapi"
	"github.com/tuxi/flux-workflow/runtime"
	"github.com/tuxi/flux-workflow/workflow"

	"github.com/stretchr/testify/require"
)

// TestFacadeSurface_SufficientForBusinessLayer is the thin-facade contract
// proof: a downstream consumer (e.g. dream-ai) that imports flux-workflow as a
// library must be able to build its own business/HTTP layer using ONLY the
// runtime facade's public surface plus direct imports of the core packages.
//
// It asserts that every handle a handler/service needs is reachable:
//   - Engine()        → power-user ops (replay, redo, cancel, fork) + TaskRepo/NodeRepo
//   - NodeRegistry()  → register custom node types / build a workflow.Builder
//   - DB()            → construct any query repository for read-side queries
//   - EventBus()      → publish custom events / attach listeners
//
// If this stops compiling or an accessor returns nil, the facade has regressed
// below what a thin-facade consumer requires.
func TestFacadeSurface_SufficientForBusinessLayer(t *testing.T) {
	rt, err := runtime.NewLocal(filepath.Join(t.TempDir(), "state.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Shutdown() })

	// 1. Core handles are all non-nil.
	require.NotNil(t, rt.Engine(), "Engine() must be exposed for power-user ops")
	require.NotNil(t, rt.NodeRegistry(), "NodeRegistry() must be exposed")
	require.NotNil(t, rt.DB(), "DB() must be exposed to build query repositories")
	require.NotNil(t, rt.EventBus(), "EventBus() must be exposed")
	require.NotNil(t, rt.ToolRegistry(), "ToolRegistry() must be shared with the business layer")

	db := rt.DB()

	// 2. From DB() a consumer can construct every read-side repository its
	//    handlers need — without the facade exposing each one individually.
	require.NotNil(t, query.NewEventRepository(db))
	require.NotNil(t, query.NewWorkflowVersionRepository(db))
	require.NotNil(t, query.NewWorkflowRepository(db))
	require.NotNil(t, query.NewNodeRuntimeRepository(db))
	require.NotNil(t, query.NewAwaitBindingRepository(db))
	require.NotNil(t, query.NewTaskCostTraceRepository(db))

	// 3. The dto-returning business query layer wraps the engine's core repo.
	taskQueryRepo := taskapi.New(db, rt.Engine().TaskRepo())
	require.NotNil(t, taskQueryRepo)

	// 4. A workflow.Builder is buildable from the node registry.
	builder := workflow.NewBuilder(rt.NodeRegistry())
	require.NotNil(t, builder)

	// 5. Engine repo accessors are reachable for handlers that need them.
	require.NotNil(t, rt.Engine().TaskRepo())
	require.NotNil(t, rt.Engine().NodeRepo())

	// 6. Subscribe + EventBus co-exist (facade convenience vs raw bus).
	ch, cancel := rt.Subscribe("task_succeeded")
	require.NotNil(t, ch)
	cancel()
}
