// Command quickstart is a self-contained demo of embedding the flux-workflow
// engine via the runtime package: no external DB, queue, or Redis required.
//
//	go run ./examples/quickstart
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tuxi/flux-workflow/runtime"

	"github.com/tuxi/flux/definition"
	"github.com/tuxi/flux/tool"
)

// greetTool is a trivial custom tool: it reads `name` from the node input and
// returns a `greeting`. Any type implementing tool.Tool can be registered.
type greetTool struct{}

func (greetTool) Name() string                  { return "greet" }
func (greetTool) Description() string            { return "greets the given name" }
func (greetTool) InputSchema() tool.DataSchema  { return tool.DataSchema{} }
func (greetTool) OutputSchema() tool.DataSchema { return tool.DataSchema{} }
func (greetTool) Mode() tool.ExecutionMode      { return tool.SyncExecution }

func (greetTool) Execute(_ context.Context, input map[string]any, _ tool.ToolEmitter) (*tool.Result, error) {
	name, _ := input["name"].(string)
	if strings.TrimSpace(name) == "" {
		name = "world"
	}
	return tool.Success(map[string]any{
		"greeting": "Hello, " + name + "!",
	}), nil
}

// greetWorkflow: start -> greet (tool) -> end.
// The greet node pulls `name` from the workflow input, and the workflow output
// surfaces the tool's greeting through the `extras` map.
func greetWorkflow() *definition.WorkflowDefinition {
	return &definition.WorkflowDefinition{
		Name: "greet_flow",
		Desc: "minimal tool workflow",
		Nodes: []definition.NodeDefinition{
			{Name: "start", Type: definition.NodeStart},
			{
				Name: "greet",
				Type: definition.NodeTool,
				Config: map[string]any{
					"tool": "greet",
				},
				InputMapping: map[string]string{
					"name": "input.name",
				},
			},
			{Name: "end", Type: definition.NodeEnd},
		},
		Edges: []definition.EdgeDefinition{
			{From: "start", To: "greet", Type: definition.EdgeNormal},
			{From: "greet", To: "end", Type: definition.EdgeNormal},
		},
		Output: definition.OutputDefinition{
			Extras: map[string]string{
				"greeting": "nodes.greet.output.greeting",
			},
		},
	}
}

func main() {
	ctx := context.Background()

	// 1. Create a self-contained local runtime (SQLite + in-memory queue/lock).
	//    Register the custom tool at construction time.
	tmpDir, err := os.MkdirTemp("", "flux-quickstart")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	rt, err := runtime.NewLocal(filepath.Join(tmpDir, "state.db"), runtime.WithLocalTool(greetTool{}))
	if err != nil {
		log.Fatalf("new local runtime: %v", err)
	}
	defer rt.Shutdown()

	// 2. Register the workflow definition (persists a version).
	def := greetWorkflow()
	if err := rt.RegisterWorkflow(ctx, def); err != nil {
		log.Fatalf("register workflow: %v", err)
	}

	// 3a. Run synchronously — returns when the DAG reaches a terminal state.
	res, err := rt.Run(ctx, def, map[string]any{"name": "flux"})
	if err != nil {
		log.Fatalf("run: %v", err)
	}
	fmt.Printf("[sync ] task=%d status=%s output=%s\n", res.TaskID, res.Status, res.Task.OutputJSON)

	// 3b. Or submit for asynchronous execution by background workers.
	if err := rt.Start(ctx, runtime.WithTaskWorkers(2)); err != nil {
		log.Fatalf("start: %v", err)
	}

	taskID, err := rt.Submit(ctx, def.Name, map[string]any{"name": "async"})
	if err != nil {
		log.Fatalf("submit: %v", err)
	}

	// Poll until the submitted task finishes (a real app would Subscribe instead).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		task, err := rt.Status(ctx, taskID)
		if err == nil && task != nil && isTerminal(string(task.Status)) {
			fmt.Printf("[async] task=%d status=%s output=%s\n", taskID, task.Status, task.OutputJSON)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	log.Fatalf("async task %d did not finish in time", taskID)
}

func isTerminal(status string) bool {
	switch status {
	case "success", "failed", "canceled":
		return true
	default:
		return false
	}
}
