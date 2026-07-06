package server

import (
	"github.com/tuxi/flux-workflow/domain"
	"github.com/tuxi/flux-workflow/eventbus"
	"sync"
	"testing"
	"time"
)

func TestAgentEventBridge_ConvertAndForward(t *testing.T) {
	bus := eventbus.NewEventBus(nil, nil)
	bridge := NewAgentEventBridge(bus)

	var mu sync.Mutex
	var received []BridgeEvent
	mockEmitter := &testBridgeEmitter{
		emit: func(e BridgeEvent) {
			mu.Lock()
			received = append(received, e)
			mu.Unlock()
		},
	}

	taskID := int64(12345)
	bridge.RegisterSession(taskID, "session-1", "turn-1", mockEmitter)

	events := []struct {
		ev   *domain.TaskEvent
		kind BridgeEventKind
	}{
		{&domain.TaskEvent{TaskID: taskID, Step: "param_validate", Type: "tool_started", NodeIndex: 1}, BridgeToolStarted},
		{&domain.TaskEvent{TaskID: taskID, Step: "param_validate", Type: "node_success", NodeIndex: 1, Message: "校验通过"}, BridgeToolFinished},
		{&domain.TaskEvent{TaskID: taskID, Step: "provider_router", Type: "tool_started", NodeIndex: 2}, BridgeToolStarted},
		{&domain.TaskEvent{TaskID: taskID, Step: "provider_router", Type: "node_success", NodeIndex: 2, Message: "路由到 aliyun"}, BridgeToolFinished},
		{&domain.TaskEvent{TaskID: taskID, Step: "image_generate_submit", Type: "tool_started", NodeIndex: 3}, BridgeToolStarted},
		{&domain.TaskEvent{TaskID: taskID, Step: "image_generate_submit", Type: "node_progress", NodeIndex: 3, Message: "已提交"}, BridgeToolStdout},
	}

	for i, tc := range events {
		bus.Publish(taskID, tc.ev)
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		c := len(received)
		mu.Unlock()
		if c < i+1 {
			t.Fatalf("step %d: expected >=%d events, got %d", i, i+1, c)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	if len(received) < len(events) {
		t.Fatalf("expected %d events, got %d", len(events), len(received))
	}
	for i, tc := range events {
		if received[i].Kind != tc.kind {
			t.Errorf("event %d: expected %s, got %s", i, tc.kind, received[i].Kind)
		}
		if received[i].ToolName != tc.ev.Step {
			t.Errorf("event %d: expected tool=%s, got %s", i, tc.ev.Step, received[i].ToolName)
		}
	}
	t.Logf("✅ Bridge: %d events converted and forwarded", len(received))
}

type testBridgeEmitter struct {
	emit func(BridgeEvent)
}

func (e *testBridgeEmitter) EmitBridgeEvent(ev BridgeEvent) { e.emit(ev) }
