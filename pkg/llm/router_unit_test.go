package llm

import (
	"context"
	"errors"
	"github.com/tuxi/flux-workflow/internal/config"
	"testing"
)

type fakeProvider struct {
	name        string
	chatFn      func(ctx context.Context, req *ChatRequest) (*ChatResponse, error)
	streamFn    func(ctx context.Context, req *ChatRequest, onChunk StreamCallback) (*ChatResponse, error)
	chatCalls   int
	streamCalls int
}

func (f *fakeProvider) Name() string { return f.name }

func (f *fakeProvider) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	f.chatCalls++
	if f.chatFn != nil {
		return f.chatFn(ctx, req)
	}
	return nil, errors.New("chat not implemented")
}

func (f *fakeProvider) ChatStream(ctx context.Context, req *ChatRequest, onChunk StreamCallback) (*ChatResponse, error) {
	f.streamCalls++
	if f.streamFn != nil {
		return f.streamFn(ctx, req, onChunk)
	}
	return nil, errors.New("stream not implemented")
}

func TestRouterCapabilityFallback(t *testing.T) {
	router := NewRouter()

	p1 := &fakeProvider{
		name: "qwen",
		chatFn: func(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
			return nil, newProviderError("qwen", req.Model, ErrorCategoryQuota, errors.New("quota exceeded"))
		},
	}
	p2 := &fakeProvider{
		name: "deepseek",
		chatFn: func(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
			return &ChatResponse{Content: "ok"}, nil
		},
	}

	router.Register("qwen-plus", p1)
	router.Register("deepseek-chat", p2)
	router.RegisterCapability(CapabilityPromptEnhance, []RouteTarget{
		{Provider: p1.Name(), Model: "qwen-plus"},
		{Provider: p2.Name(), Model: "deepseek-chat"},
	})

	resp, err := router.Chat(context.Background(), &ChatRequest{
		Capability: CapabilityPromptEnhance,
		Messages:   []Message{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("router.Chat() error = %v", err)
	}
	if resp.Provider != "deepseek" || resp.Model != "deepseek-chat" {
		t.Fatalf("unexpected route: provider=%s model=%s", resp.Provider, resp.Model)
	}
	if resp.RequestedCapability != CapabilityPromptEnhance || resp.FinalProvider != "deepseek" || resp.FinalModel != "deepseek-chat" || resp.FallbackHops != 1 {
		t.Fatalf("unexpected metadata: %+v", resp)
	}
	if p1.chatCalls != 1 || p2.chatCalls != 1 {
		t.Fatalf("unexpected calls: p1=%d p2=%d", p1.chatCalls, p2.chatCalls)
	}
}

func TestRouterChatStreamFallback(t *testing.T) {
	router := NewRouter()

	p1 := &fakeProvider{
		name: "qwen",
		streamFn: func(ctx context.Context, req *ChatRequest, onChunk StreamCallback) (*ChatResponse, error) {
			return nil, newProviderError("qwen", req.Model, ErrorCategoryTimeout, errors.New("timeout"))
		},
		chatFn: func(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
			return nil, newProviderError("qwen", req.Model, ErrorCategoryTimeout, errors.New("timeout"))
		},
	}
	p2 := &fakeProvider{
		name: "deepseek",
		streamFn: func(ctx context.Context, req *ChatRequest, onChunk StreamCallback) (*ChatResponse, error) {
			onChunk("hello")
			onChunk(" world")
			return &ChatResponse{Content: "hello world"}, nil
		},
	}

	router.Register("qwen-plus", p1)
	router.Register("deepseek-chat", p2)
	router.RegisterCapability(CapabilityPromptEnhance, []RouteTarget{
		{Provider: p1.Name(), Model: "qwen-plus"},
		{Provider: p2.Name(), Model: "deepseek-chat"},
	})

	var got string
	resp, err := router.ChatStream(context.Background(), &ChatRequest{
		Capability: CapabilityPromptEnhance,
		Messages:   []Message{{Role: "user", Content: "hi"}},
	}, func(chunk string) {
		got += chunk
	})
	if err != nil {
		t.Fatalf("router.ChatStream() error = %v", err)
	}
	if got != "hello world" || resp.Content != "hello world" {
		t.Fatalf("unexpected stream output: got=%q resp=%q", got, resp.Content)
	}
	if resp.RequestedCapability != CapabilityPromptEnhance || resp.FinalProvider != "deepseek" || resp.FinalModel != "deepseek-chat" || resp.FallbackHops != 1 {
		t.Fatalf("unexpected metadata: %+v", resp)
	}
	if p1.streamCalls != 1 || p1.chatCalls != 1 || p2.streamCalls != 1 {
		t.Fatalf("unexpected calls: p1.stream=%d p1.chat=%d p2.stream=%d", p1.streamCalls, p1.chatCalls, p2.streamCalls)
	}
}

func TestRouterCircuitBreakerSkipsOpenTarget(t *testing.T) {
	router := NewRouter()

	p1 := &fakeProvider{
		name: "qwen",
		chatFn: func(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
			return nil, newProviderError("qwen", req.Model, ErrorCategoryQuota, errors.New("quota exceeded"))
		},
	}
	p2 := &fakeProvider{
		name: "deepseek",
		chatFn: func(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
			return &ChatResponse{Content: "ok"}, nil
		},
	}

	router.Register("qwen-plus", p1)
	router.Register("deepseek-chat", p2)
	router.RegisterCapability(CapabilityPromptEnhance, []RouteTarget{
		{Provider: p1.Name(), Model: "qwen-plus"},
		{Provider: p2.Name(), Model: "deepseek-chat"},
	})

	req := &ChatRequest{
		Capability: CapabilityPromptEnhance,
		Messages:   []Message{{Role: "user", Content: "hi"}},
	}

	if _, err := router.Chat(context.Background(), req); err != nil {
		t.Fatalf("first router.Chat() error = %v", err)
	}
	if _, err := router.Chat(context.Background(), req); err != nil {
		t.Fatalf("second router.Chat() error = %v", err)
	}

	if p1.chatCalls != 1 {
		t.Fatalf("breaker did not skip open target, qwen calls=%d", p1.chatCalls)
	}
	if p2.chatCalls != 2 {
		t.Fatalf("unexpected fallback calls, deepseek calls=%d", p2.chatCalls)
	}
}

func TestBuildDefaultRouterRegistersCapabilities(t *testing.T) {
	cfg := &config.Config{
		DeepSeek: config.LLM{ApiKey: "k1", BaseURL: "https://deepseek.example.com"},
		OpenAI:   config.LLM{ApiKey: "k2", BaseURL: "https://openai.example.com"},
		Qwen:     config.LLM{ApiKey: "k3", BaseURL: "https://qwen.example.com"},
	}

	router := BuildDefaultRouter(cfg)
	targets := router.resolveTargets(&ChatRequest{Capability: CapabilityPromptEnhance})
	if len(targets) != 3 {
		t.Fatalf("expected 3 default targets, got %d", len(targets))
	}
	if targets[0].Provider != "qwen" || targets[0].Model != "qwen-plus" {
		t.Fatalf("unexpected first target: %+v", targets[0])
	}
}
