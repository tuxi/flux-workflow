package agent

import (
	"context"
	"encoding/json"
	"log/slog"
)

// planWorkflowToolName is the single client tool exposed in the minimal loop.
// codeagentd's model calls it with {"goal": "..."}; the relay runs it in-process
// via PlanRunner (which drives the v1 engine) and returns the final result.
//
// NOTE: the name must NOT collide with codeagentd's own built-in tools. codeagentd
// already registers a native server-side "plan_workflow" (its embedded flux), and
// a client tool that collides is silently dropped (serve_builder skips on name
// clash) — the server tool then runs instead and the DreamAI engine never fires.
// So we use a DreamAI-specific name.
const planWorkflowToolName = "dreamai_generate"

// planWorkflowSchema is the JSON Schema advertised to the model for the tool.
var planWorkflowSchema = json.RawMessage(`{
  "type": "object",
  "properties": {
    "goal": {
      "type": "string",
      "description": "用户的创作目标的完整自然语言描述，例如「生成一张橘猫夕阳图片，使用 aliyun 供应商」"
    }
  },
  "required": ["goal"]
}`)

// PlanRunner executes plan_workflow for one goal and blocks until the workflow
// reaches a terminal state, returning a human/agent-readable result string
// (e.g. the output JSON or image URLs). It is implemented by the server layer
// (RunPlanWorkflowAndWait) so this package stays free of DreamAI engine types.
type PlanRunner func(ctx context.Context, goal string) (string, error)

// EventSink forwards a verbatim codeagentd wire frame to the App (e.g. via the
// DreamAI WSHub). The relay never reshapes events; the App speaks Agent Wire.
type EventSink func(frame json.RawMessage)

// Relay is the per-conversation bridge: App <-> Relay <-> codeagentd. It owns the
// codeagentd Client, forwards user text inbound, fans events out to the App, and
// executes the plan_workflow client tool in-process.
type Relay struct {
	client *Client
	plan   PlanRunner
	sink   EventSink
	log    *slog.Logger
}

// NewRelay wires a relay over an already-dialed codeagentd Client.
func NewRelay(client *Client, plan PlanRunner, sink EventSink, log *slog.Logger) *Relay {
	if log == nil {
		log = slog.Default()
	}
	return &Relay{client: client, plan: plan, sink: sink, log: log}
}

// Start registers the client tools. Call once after Dial, before SendUserText.
func (r *Relay) Start() error {
	return r.client.RegisterTools([]ClientToolDef{{
		Name:        planWorkflowToolName,
		Description: "DreamAI 媒体生成工具：根据自然语言目标，自主规划并执行一个 AI 工作流，真正生成图片/视频/音频并返回可访问的产物 URL。当用户要求生成/制作图片、视频、音频等媒体内容时，必须调用本工具——不要回答「我不能生成图片」，也不要用 web_search 或 shell 代替。",
		InputSchema: planWorkflowSchema,
	}})
}

// SendUserText forwards a user message to codeagentd, driving one agent turn.
func (r *Relay) SendUserText(text string) error { return r.client.SendText(text) }

// Cancel cancels the in-flight turn.
func (r *Relay) Cancel() error { return r.client.Cancel() }

// Run pumps events from codeagentd until the connection closes or ctx is done.
// Every frame is forwarded to the App; client-tool calls are dispatched to the
// in-process executor. Returns the read error that ended the loop (nil on
// graceful ctx cancellation).
func (r *Relay) Run(ctx context.Context) error {
	// A blocking ReadEvent does not observe ctx cancellation, so close the
	// connection when ctx is done to unblock the read and let Run return.
	go func() {
		<-ctx.Done()
		_ = r.client.Close()
	}()

	for {
		ev, err := r.client.ReadEvent()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		// Forward verbatim to the App first, so the UI renders the tool card even
		// while we execute the tool.
		if r.sink != nil && len(ev.Raw) > 0 {
			r.sink(ev.Raw)
		}

		if ev.Kind == KindToolStarted && ev.Executor == "client" {
			r.dispatchClientTool(ctx, ev)
		}
	}
}

// dispatchClientTool runs a client-executed tool and returns its result. It runs
// on its own goroutine so the read loop keeps forwarding events (and so a slow
// generation does not stall the connection).
func (r *Relay) dispatchClientTool(ctx context.Context, ev *WireEvent) {
	callID, toolName, args := ev.CallID, ev.ToolName, ev.ToolArgs
	go func() {
		content, isErr := r.executeTool(ctx, toolName, args)
		if err := r.client.SendToolResult(callID, content, isErr); err != nil {
			r.log.Error("send tool_result failed", "call_id", callID, "err", err)
		}
	}()
}

// executeTool is the JSON boundary: it decodes the wire args and invokes the
// in-process implementation. For the minimal loop only plan_workflow is wired.
func (r *Relay) executeTool(ctx context.Context, name string, args json.RawMessage) (content string, isError bool) {
	switch name {
	case planWorkflowToolName:
		var in struct {
			Goal string `json:"goal"`
		}
		if err := json.Unmarshal(args, &in); err != nil || in.Goal == "" {
			return "tool error: plan_workflow 需要非空的 goal 参数", true
		}
		result, err := r.plan(ctx, in.Goal)
		if err != nil {
			r.log.Error("plan_workflow failed", "err", err)
			return "tool error: " + err.Error(), true
		}
		return result, false
	default:
		return "tool error: unknown client tool: " + name, true
	}
}
