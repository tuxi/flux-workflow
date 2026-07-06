# flux-workflow

An **embeddable, durable DAG workflow engine for Go**.

flux-workflow runs directed-acyclic-graph workflows and owns their runtime state
itself. You embed it as a library, point it at a database, register your tools
and workflows, and submit tasks — the engine handles execution, persistence,
crash recovery, and human-in-the-loop suspension. It is designed for AI / tool
orchestration pipelines but knows nothing about any particular domain.

```go
rt, _ := runtime.NewLocal("state.db", runtime.WithLocalTool(myTool))
rt.RegisterWorkflow(ctx, def)
res, _ := rt.Run(ctx, def, map[string]any{"name": "flux"})
fmt.Println(res.Status) // success
```

## Why

| | flux-workflow |
|---|---|
| **Embeddable** | A Go library, not a server. No sidecar to deploy for local use. |
| **Durable** | Runtime state (tasks, node runtimes, events, await bindings) lives in *your* database — SQLite for local, Postgres for production. |
| **Workflow-as-data** | Workflows are DAG definitions (JSON/DSL), versioned by content hash on registration. |
| **Crash-safe** | Interrupted runs are recovered by a scanner; failed tasks can be retried from the failed subtree. |
| **Human-in-the-loop** | `await` nodes suspend a run until an external signal (webhook, approval, async job) resumes it. |
| **Pluggable infra** | Database, task queue, distributed lock, event bus and job queue are all injected. |

Closest relatives: DBOS Transact (durable execution as a library), LangGraph
(graph + checkpointer + interrupts), Temporal/Cadence (signals ≈ resume, event
history ≈ persistent events) — but those are either not Go, or require a server.

## Install

```sh
go get github.com/tuxi/flux-workflow
```

Requires Go 1.25+.

## Quickstart

A complete, self-contained example (SQLite + in-memory queue, no external
services) lives in [`examples/quickstart`](examples/quickstart/main.go):

```sh
go run ./examples/quickstart
```

```
[sync ] task=... status=success output={"final":{...,"extras":{"greeting":"Hello, flux!"}}}
[async] task=... status=success output={"final":{...,"extras":{"greeting":"Hello, async!"}}}
```

It defines a `greet` tool, wires a `start → greet → end` workflow, and runs it
both synchronously (`Run`) and asynchronously via background workers
(`Start` + `Submit`).

## The Runtime API

`runtime.Runtime` is the single entry point. Everything you need is a method on it.

### Construction

```go
// Local mode — self-contained: SQLite (WAL), in-memory queue / job queue / lock,
// no external dependencies. Ideal for embedding, tests, and single-process apps.
rt, err := runtime.NewLocal("state.db", runtime.WithLocalTool(myTool))

// Server mode — you inject the infrastructure.
rt, err := runtime.New(
    runtime.WithDB(pgDB),          // *gorm.DB (Postgres or SQLite)
    runtime.WithQueue(redisQ),     // repository.TaskQueue
    runtime.WithJobQueue(jobQ),    // engine.AsyncJobQueue
    runtime.WithEventBus(bus),     // *eventbus.EventBus
    runtime.WithLock(dLock),       // lock.DistributedLock
    runtime.WithTool(myTool),
)
```

### Lifecycle & execution

| Method | Purpose |
|---|---|
| `RegisterWorkflow(ctx, def)` | Persist a workflow definition; re-publishes a version when its content hash changes. |
| `Run(ctx, def, input)` | Execute synchronously; returns when the DAG reaches a terminal or suspended state. |
| `Start(ctx, opts...)` | Launch background workers (task / async / await-poll) + recovery scanner. |
| `Submit(ctx, name, input)` | Enqueue a task by workflow name for asynchronous execution. Requires `Start`. |
| `Status(ctx, taskID)` | Fetch current task state. |
| `Resume(ctx, taskID, node, meta)` | Wake a suspended `await` node with an external result; continues the DAG. Idempotent. |
| `Retry(ctx, taskID, from, patches)` | Manually recover a failed/canceled/suspended task; resets the failed subtree and re-enqueues. |
| `Subscribe(eventType)` | Get a channel of `*domain.TaskEvent` plus an unsubscribe func. |
| `Shutdown()` | Stop workers, wait for them to exit, close the owned DB. |

Worker concurrency is tunable:

```go
rt.Start(ctx,
    runtime.WithTaskWorkers(4),
    runtime.WithAsyncWorkers(4),
    runtime.WithAwaitPollWorkers(1),
)
```

### Building a service layer on top

The facade covers the common path. To build your own HTTP/service layer, reach
the underlying handles and use the core packages directly:

```go
db  := rt.DB()            // *gorm.DB — construct any query repo from repository/query
bus := rt.EventBus()      // publish custom events / attach listeners
eng := rt.Engine()        // power-user ops: Replay, Redo, Cancel, Fork, ...
reg := rt.NodeRegistry()  // register custom node types
```

The `runtime` facade is deliberately thin: advanced operations stay on the core
packages (`engine`, `repository`, `eventbus`) rather than being mirrored here.

### Human-in-the-loop

An `await` node suspends the task. When the external event arrives, resume it:

```go
res, _ := rt.Run(ctx, def, input)   // res.Status == "suspended"
// ... webhook / approval arrives ...
rt.Resume(ctx, res.TaskID, "wait_approval", map[string]any{"approved": true})
```

## Workflows & node types

A workflow is a `definition.WorkflowDefinition` (from `github.com/tuxi/flux`):
`Nodes`, `Edges`, and an `Output` mapping. Built-in node types:

| Type | Role |
|---|---|
| `start` / `end` | DAG entry / exit. |
| `tool` | Runs a registered `tool.Tool` (`config.tool` selects it). |
| `await` | Suspends until an external signal / async completion resumes it. |
| `map` | Fans out over a collection, one child sub-task per item. |
| `loop` | Iterates a sub-workflow with carry-over state. |
| `subworkflow` | Runs another registered workflow as a child. |

Node inputs are expressions (`input.name`, `nodes.greet.output.greeting`); the
workflow `Output` maps node outputs into the final result. Register your own
node types via `rt.NodeRegistry()`.

## Architecture

```
runtime/                one-stop facade (this is the public API)
engine/                 DAG execution, resume, retry, replay, fork, cancel
workflow/, workflow/nodes/  builder + node implementations
repository/, repository/query/  storage interfaces + GORM implementation
eventbus/               layered event routing (transient / persistent / audit)
worker/                 task, async, await-poll workers + recovery scanner
domain/                 core entities
```

The core is dependency-light: `go list -deps ./runtime` pulls **no** web
framework, cloud SDK, or LLM client. Redis-backed drivers are **optional
subpackages** so pure local embedding never links go-redis:

- `engine/redisjobqueue` — Redis Stream `AsyncJobQueue`
- `repository/query/redisqueue` — Redis `TaskQueue`
- `pkg/lock/redislock` — Redsync `DistributedLock`

## Status

flux-workflow was extracted from a production AI product and is being hardened
for general use. The engine core and the `runtime` facade are stable; some
higher-level HTTP/presentation code is still being separated out. APIs may shift
before a tagged release.

## License

[MIT](LICENSE)
