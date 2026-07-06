package agent

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestAgentE2E drives the live /agent/ws endpoint against a running DreamAI +
// codeagentd. Opt-in: set AGENT_E2E_URL to the ws URL, e.g.
//
//	AGENT_E2E_URL=ws://127.0.0.1:12210/api/v1/ai/agent/ws \
//	  go test ./ai-engine/agent/ -run TestAgentE2E -v -timeout 600s
func TestAgentE2E(t *testing.T) {
	url := os.Getenv("AGENT_E2E_URL")
	if url == "" {
		t.Skip("set AGENT_E2E_URL to run the live end-to-end test")
	}
	goal := os.Getenv("AGENT_E2E_GOAL")
	if goal == "" {
		goal = "生成一张橘猫夕阳图片"
	}

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"type": "send_message", "text": goal}); err != nil {
		t.Fatalf("send: %v", err)
	}
	t.Logf(">>> sent goal: %s", goal)

	deadline := time.Now().Add(9 * time.Minute)
	_ = conn.SetReadDeadline(deadline)

	var sawPlanTool, sawTurnFinished bool
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			t.Logf("<<< read ended: %v", err)
			break
		}
		var ev WireEvent
		_ = json.Unmarshal(data, &ev)

		summary := ev.Text
		if ev.Observation != "" {
			summary = ev.Observation
		}
		if len(summary) > 200 {
			summary = summary[:200] + "…"
		}
		t.Logf("<<< kind=%-15s tool=%-16s exec=%-6s %s", ev.Kind, ev.ToolName, ev.Executor, summary)

		if ev.ToolName == planWorkflowToolName {
			sawPlanTool = true
		}
		if ev.Kind == KindTurnFinished {
			sawTurnFinished = true
			break
		}
		if ev.Kind == KindError {
			t.Fatalf("agent error event: %s", string(data))
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for turn_finished")
		}
	}

	if !sawPlanTool {
		t.Error("never saw plan_workflow tool invocation")
	}
	if !sawTurnFinished {
		t.Error("never saw turn_finished")
	}
}
