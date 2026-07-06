package server

import (
	"encoding/json"
	"fmt"
	"github.com/tuxi/flux-workflow/domain"
	"github.com/tuxi/flux-workflow/eventbus"
	"sync"
	"time"
)

// BridgeEvent is DreamAI's standalone event type, mirrors code-agent's agent.Event
// without importing code-agent's internal package.
type BridgeEventKind string

const (
	BridgeToolStarted  BridgeEventKind = "tool_started"
	BridgeToolFinished BridgeEventKind = "tool_finished"
	BridgeToolStdout   BridgeEventKind = "tool_stdout"
	BridgeTurnFinished BridgeEventKind = "turn_finished"
)

type BridgeEvent struct {
	Kind        BridgeEventKind `json:"kind"`
	At          time.Time       `json:"at"`
	SessionID   string          `json:"session_id"`
	TurnID      string          `json:"turn_id"`
	ToolName    string          `json:"tool_name,omitempty"`
	Step        int             `json:"step,omitempty"`
	ToolArgs    string          `json:"tool_args,omitempty"`
	Observation string          `json:"observation,omitempty"`
	Chunk       string          `json:"chunk,omitempty"`
	Text        string          `json:"text,omitempty"`
	Err         string          `json:"err,omitempty"`
}

// BridgeEmitter receives bridge events. Implementations serialize to WS, SSE, etc.
type BridgeEmitter interface {
	EmitBridgeEvent(BridgeEvent)
}

// AgentEventBridge bridges DreamAI task_events to BridgeEvents.
// Subscribes to EventBus and converts domain.TaskEvent → BridgeEvent.
type AgentEventBridge struct {
	mu       sync.RWMutex
	sessions map[int64]*bridgedSession
}

type bridgedSession struct {
	sessionID string
	turnID    string
	emitter   BridgeEmitter
}

func NewAgentEventBridge(bus *eventbus.EventBus) *AgentEventBridge {
	b := &AgentEventBridge{
		sessions: make(map[int64]*bridgedSession),
	}
	// Subscribe to all relevant workflow event types
	for _, et := range []string{
		"tool_started", "node_started",
		"tool_completed", "node_success",
		"tool_failed", "node_failed",
		"node_progress",
		"task_completed", "task_failed",
		"tool_stream", "tool_log", "tool_progress",
	} {
		go b.forwardLoop(bus.Subscribe(et))
	}
	return b
}

func (b *AgentEventBridge) RegisterSession(taskID int64, sessionID, turnID string, emitter BridgeEmitter) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sessions[taskID] = &bridgedSession{sessionID: sessionID, turnID: turnID, emitter: emitter}
}

func (b *AgentEventBridge) UnregisterSession(taskID int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.sessions, taskID)
}

func (b *AgentEventBridge) forwardLoop(ch <-chan *domain.TaskEvent) {
	for ev := range ch {
		if ev == nil {
			continue
		}
		b.mu.RLock()
		sess, ok := b.sessions[ev.TaskID]
		if !ok {
			sess, ok = b.sessions[ev.RootTaskID]
		}
		b.mu.RUnlock()
		if !ok || sess == nil || sess.emitter == nil {
			continue
		}
		be := b.convert(ev, sess)
		sess.emitter.EmitBridgeEvent(be)
	}
}

func (b *AgentEventBridge) convert(ev *domain.TaskEvent, sess *bridgedSession) BridgeEvent {
	be := BridgeEvent{
		At:        time.Now(),
		SessionID: sess.sessionID,
		TurnID:    sess.turnID,
		ToolName:  ev.Step,
		Step:      ev.NodeIndex,
	}

	switch ev.Type {
	case "tool_started", "node_started":
		be.Kind = BridgeToolStarted
		if ev.Meta != nil {
			if args, _ := json.Marshal(ev.Meta); len(args) > 0 {
				be.ToolArgs = string(args)
			}
		}

	case "tool_completed", "node_success":
		be.Kind = BridgeToolFinished
		be.Observation = ev.Message

	case "tool_failed", "node_failed":
		be.Kind = BridgeToolFinished
		be.Err = ev.Error
		be.Observation = fmt.Sprintf("failed: %s", ev.Error)

	case "node_progress":
		be.Kind = BridgeToolStdout
		be.Chunk = ev.Message

	case "task_completed":
		be.Kind = BridgeTurnFinished
		be.Text = ev.Message

	case "task_failed":
		be.Kind = BridgeTurnFinished
		be.Err = ev.Error
		be.Text = ev.Message

	default:
		be.Kind = BridgeToolStdout
		be.Chunk = fmt.Sprintf("[%s] %s", ev.Type, ev.Message)
	}

	return be
}
