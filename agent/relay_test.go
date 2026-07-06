package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeCodeagentd is a minimal stand-in for codeagentd: it serves
// POST /v1/conversations and the WS stream, and scripts one client-tool round
// trip so we can assert the relay's wire behavior end to end without an LLM.
type fakeCodeagentd struct {
	t        *testing.T
	upgrader websocket.Upgrader

	mu             sync.Mutex
	registeredTool string // first tool name seen in register_tools
	gotGoalText    string // text seen in agent_input{kind:text}
	gotToolResult  *ToolResult
}

func (f *fakeCodeagentd) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/conversations", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"id": "conv_test_1"})
	})
	mux.HandleFunc("GET /v1/conversations/{id}/stream", f.serveStream)
	return mux
}

func (f *fakeCodeagentd) serveStream(w http.ResponseWriter, r *http.Request) {
	conn, err := f.upgrader.Upgrade(w, r, nil)
	if err != nil {
		f.t.Errorf("server upgrade: %v", err)
		return
	}
	defer conn.Close()

	// 1. hello handshake.
	_ = conn.WriteJSON(map[string]any{"type": "hello", "protocol_version": 1, "capabilities": []string{"client_tool_execution"}})

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var env struct {
			Type       string          `json:"type"`
			Kind       string          `json:"kind"`
			Text       string          `json:"text"`
			Tools      []ClientToolDef `json:"tools"`
			ToolResult *ToolResult     `json:"tool_result"`
		}
		if json.Unmarshal(data, &env) != nil {
			continue
		}
		switch {
		case env.Type == "register_tools":
			f.mu.Lock()
			if len(env.Tools) > 0 {
				f.registeredTool = env.Tools[0].Name
			}
			f.mu.Unlock()

		case env.Type == "agent_input" && env.Kind == "text":
			f.mu.Lock()
			f.gotGoalText = env.Text
			f.mu.Unlock()
			// Simulate the model deciding to call the client tool.
			_ = conn.WriteJSON(map[string]any{
				"kind":      KindToolStarted,
				"call_id":   "call_42",
				"tool_name": planWorkflowToolName,
				"tool_args": json.RawMessage(`{"goal":"` + env.Text + `"}`),
				"executor":  "client",
			})

		case env.Type == "agent_input" && env.Kind == "tool_result":
			f.mu.Lock()
			f.gotToolResult = env.ToolResult
			f.mu.Unlock()
			// Close out the turn.
			_ = conn.WriteJSON(map[string]any{"kind": KindTurnFinished, "text": "done"})
		}
	}
}

func TestRelay_ClientToolRoundTrip(t *testing.T) {
	fake := &fakeCodeagentd{t: t}
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := Dial(ctx, srv.URL, ".")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.Close()

	var planGoal string
	plan := func(_ context.Context, goal string) (string, error) {
		planGoal = goal
		return "工作流执行成功，产物 https://example.com/cat.png", nil
	}

	var sinkMu sync.Mutex
	var kinds []string
	sink := func(frame json.RawMessage) {
		var ev WireEvent
		_ = json.Unmarshal(frame, &ev)
		sinkMu.Lock()
		kinds = append(kinds, ev.Kind)
		sinkMu.Unlock()
	}

	relay := NewRelay(cli, plan, sink, nil)
	if err := relay.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	done := make(chan struct{})
	go func() { _ = relay.Run(ctx); close(done) }()

	const goal = "生成一张橘猫夕阳图片"
	if err := relay.SendUserText(goal); err != nil {
		t.Fatalf("SendUserText: %v", err)
	}

	// Wait for the turn to finish (or timeout).
	if !waitForKind(&sinkMu, &kinds, KindTurnFinished, 4*time.Second) {
		t.Fatalf("did not observe turn_finished; saw kinds=%v", kinds)
	}
	cancel()
	<-done

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.registeredTool != planWorkflowToolName {
		t.Errorf("registered tool = %q, want %q", fake.registeredTool, planWorkflowToolName)
	}
	if fake.gotGoalText != goal {
		t.Errorf("forwarded text = %q, want %q", fake.gotGoalText, goal)
	}
	if planGoal != goal {
		t.Errorf("plan runner goal = %q, want %q", planGoal, goal)
	}
	if fake.gotToolResult == nil {
		t.Fatal("server never received tool_result")
	}
	if fake.gotToolResult.ToolUseID != "call_42" {
		t.Errorf("tool_result.tool_use_id = %q, want call_42", fake.gotToolResult.ToolUseID)
	}
	if fake.gotToolResult.IsError {
		t.Errorf("tool_result.is_error = true, want false")
	}
	if !strings.Contains(fake.gotToolResult.Content, "cat.png") {
		t.Errorf("tool_result.content = %q, missing plan output", fake.gotToolResult.Content)
	}
}

func waitForKind(mu *sync.Mutex, kinds *[]string, want string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		mu.Lock()
		for _, k := range *kinds {
			if k == want {
				mu.Unlock()
				return true
			}
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
