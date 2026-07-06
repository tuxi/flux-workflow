// Package agent bridges DreamAI to the codeagentd sidecar over the Agent Wire
// Protocol. DreamAI is the *single* client of codeagentd (see
// ai-engine/docs/dreamai-agent-integration.md §2): it drives the conversation
// (forwards user text, relays events) AND executes client-side tools (the
// DreamAI Provider tools / plan_workflow run in this process and the result is
// sent back over the wire).
//
// This file holds the low-level wire client. The relay logic (tool execution,
// event fan-out to the App) lives in relay.go.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ClientToolDef mirrors code-agent's agent.ClientToolDef. Sent in register_tools
// so codeagentd advertises the tool to the model as a client-executed tool.
type ClientToolDef struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

// ToolResult is the client-tool execution result payload carried inside an
// agent_input frame of kind "tool_result" (mirrors code-agent server.ToolResult).
type ToolResult struct {
	ToolUseID string `json:"tool_use_id"`
	Subtype   string `json:"subtype"`
	Content   string `json:"content,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
}

// WireEvent is the subset of codeagentd's outbound wire event we consume. The
// full schema is in code-agent/internal/server/wire.go; unknown fields are
// ignored. Kind selects which fields are meaningful.
type WireEvent struct {
	Kind        string          `json:"kind"`
	CallID      string          `json:"call_id"`
	Step        int             `json:"step"`
	ToolName    string          `json:"tool_name"`
	ToolArgs    json.RawMessage `json:"tool_args"`
	Observation string          `json:"observation"`
	Executor    string          `json:"executor"` // "client" => this side must execute the tool
	Text        string          `json:"text"`
	Err         string          `json:"err"`
	Raw         json.RawMessage `json:"-"` // the original frame, for verbatim fan-out to the App
}

// Event kinds we branch on. The full set is larger; we only name what we use.
const (
	KindToolStarted  = "tool_started"
	KindTurnFinished = "turn_finished"
	KindError        = "error"
)

// Client is a single Agent Wire Protocol connection to codeagentd, scoped to one
// conversation. It is safe for one reader (ReadEvent) and concurrent writers
// (writes are serialized by a mutex).
type Client struct {
	httpBase string // e.g. http://127.0.0.1:8787
	convID   string

	conn   *websocket.Conn
	writeM sync.Mutex
}

// Dial creates a conversation on codeagentd and opens its event/command stream.
// workspacePath is the codeagentd workspace the session is rooted in (unused by
// DreamAI tools, but required by the conversation model).
func Dial(ctx context.Context, httpBase, workspacePath string) (*Client, error) {
	httpBase = strings.TrimRight(httpBase, "/")

	convID, err := createConversation(ctx, httpBase, workspacePath)
	if err != nil {
		return nil, fmt.Errorf("create conversation: %w", err)
	}

	wsURL, err := toWebSocketURL(httpBase, "/v1/conversations/"+convID+"/stream")
	if err != nil {
		return nil, err
	}

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("ws dial %s: %w", wsURL, err)
	}

	c := &Client{httpBase: httpBase, convID: convID, conn: conn}

	// First frame is the hello handshake. Read and discard it; we do not depend
	// on a specific capability for the minimal loop.
	if _, _, err := c.conn.ReadMessage(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read hello: %w", err)
	}
	return c, nil
}

func (c *Client) ConversationID() string { return c.convID }

// RegisterTools declares the client-executed tools to codeagentd. Must be sent
// after Dial (post-hello) and before the first user turn.
func (c *Client) RegisterTools(defs []ClientToolDef) error {
	return c.writeJSON(map[string]any{
		"type":  "register_tools",
		"tools": defs,
	})
}

// SendText forwards a user message, driving one agent turn.
func (c *Client) SendText(text string) error {
	return c.writeJSON(map[string]any{
		"type": "agent_input",
		"kind": "text",
		"text": text,
	})
}

// SendToolResult returns a client-tool execution result, keyed by the call_id
// from the tool_started event that requested it.
func (c *Client) SendToolResult(callID, content string, isError bool) error {
	return c.writeJSON(map[string]any{
		"type": "agent_input",
		"kind": "tool_result",
		"tool_result": ToolResult{
			ToolUseID: callID,
			Subtype:   "result",
			Content:   content,
			IsError:   isError,
		},
	})
}

// Cancel cancels the in-flight turn.
func (c *Client) Cancel() error {
	return c.writeJSON(map[string]any{"type": "agent_input", "kind": "command", "text": "cancel"})
}

// ReadEvent blocks for the next wire event. Returns io.EOF-style errors when the
// connection closes; callers treat any error as terminal for the connection.
func (c *Client) ReadEvent() (*WireEvent, error) {
	_, data, err := c.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	var ev WireEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		// Not all frames are events we model; surface the raw bytes anyway so the
		// caller can still forward them.
		return &WireEvent{Raw: append(json.RawMessage(nil), data...)}, nil
	}
	ev.Raw = append(json.RawMessage(nil), data...)
	return &ev, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) writeJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.writeM.Lock()
	defer c.writeM.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

// localHTTPClient talks to the codeagentd sidecar directly. The sidecar is
// always a local/direct connection, so we explicitly bypass any HTTP(S)_PROXY in
// the environment — otherwise a localhost request gets routed through a proxy
// (and typically 502s). The WS dialer (websocket.Dialer{}) already has a nil
// Proxy, so it needs no equivalent.
var localHTTPClient = &http.Client{
	Timeout:   30 * time.Second,
	Transport: &http.Transport{Proxy: nil},
}

// createConversation POSTs /v1/conversations and returns the new session id.
func createConversation(ctx context.Context, httpBase, workspacePath string) (string, error) {
	body, _ := json.Marshal(map[string]string{"workspace_path": workspacePath})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, httpBase+"/v1/conversations", strings.NewReader(string(body)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := localHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("status %d: %s", resp.StatusCode, string(raw))
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.ID == "" {
		return "", fmt.Errorf("unexpected response: %s", string(raw))
	}
	return out.ID, nil
}

// toWebSocketURL rewrites an http(s) base + path into a ws(s) URL.
func toWebSocketURL(httpBase, path string) (string, error) {
	u, err := url.Parse(httpBase)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "http", "":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	u.Path = strings.TrimRight(u.Path, "/") + path
	return u.String(), nil
}
