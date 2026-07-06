package llm

import (
	"context"
	"errors"
	"strings"
	"sync"
)

type RouteTarget struct {
	Provider string
	Model    string
}

type Router struct {
	mu        sync.RWMutex
	providers map[string]Provider
	routes    map[string][]RouteTarget
	fallback  []string
	breaker   *simpleBreaker
}

func CapabilityRouteKey(capability string) string {
	return "cap:" + strings.TrimSpace(capability)
}

func (r *Router) Register(model string, p Provider) {
	if r == nil || p == nil {
		return
	}

	model = strings.TrimSpace(model)

	r.mu.Lock()
	defer r.mu.Unlock()

	r.providers[p.Name()] = p
	r.routes[model] = []RouteTarget{{
		Provider: p.Name(),
		Model:    model,
	}}
}

func (r *Router) RegisterRoute(key string, targets []RouteTarget) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes[strings.TrimSpace(key)] = normalizeRouteTargets(targets)
}

func (r *Router) RegisterCapability(capability string, targets []RouteTarget) {
	r.RegisterRoute(CapabilityRouteKey(capability), targets)
}

func (r *Router) SetFallback(models []string) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.fallback = append([]string{}, models...)
}

func (r *Router) Chat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	return r.executeChat(ctx, req)
}

func (r *Router) ChatStream(
	ctx context.Context,
	req *ChatRequest,
	onChunk StreamCallback,
) (*ChatResponse, error) {
	return r.executeChatStream(ctx, req, onChunk)
}

func (r *Router) executeChat(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	targets := r.resolveTargets(req)
	if len(targets) == 0 {
		return nil, errors.New("no available llm provider")
	}

	var lastErr error
	for idx, target := range targets {
		if !r.allowTarget(target) {
			lastErr = newProviderError(target.Provider, target.Model, ErrorCategoryUnavailable, ErrCircuitOpen)
			continue
		}

		provider, ok := r.getProvider(target.Provider)
		if !ok {
			continue
		}

		resp, err := provider.Chat(ctx, withRouteTarget(req, target))
		if err == nil {
			r.onTargetSuccess(target)
			return finalizeResponse(resp, req, target, idx), nil
		}

		lastErr = normalizeProviderError(err, target.Provider, target.Model)
		r.onTargetFailure(target, lastErr)
		if !shouldContinueFallback(lastErr) {
			return nil, lastErr
		}
	}

	if lastErr == nil {
		lastErr = errors.New("no available llm provider")
	}
	return nil, lastErr
}

func (r *Router) executeChatStream(
	ctx context.Context,
	req *ChatRequest,
	onChunk StreamCallback,
) (*ChatResponse, error) {
	targets := r.resolveTargets(req)
	if len(targets) == 0 {
		return nil, errors.New("no available llm provider")
	}

	var lastErr error
	for idx, target := range targets {
		if !r.allowTarget(target) {
			lastErr = newProviderError(target.Provider, target.Model, ErrorCategoryUnavailable, ErrCircuitOpen)
			continue
		}

		provider, ok := r.getProvider(target.Provider)
		if !ok {
			continue
		}

		attemptReq := withRouteTarget(req, target)
		streamedAny := false
		resp, err := provider.ChatStream(ctx, attemptReq, func(chunk string) {
			if chunk == "" {
				return
			}
			streamedAny = true
			if onChunk != nil {
				onChunk(chunk)
			}
		})
		if err == nil {
			r.onTargetSuccess(target)
			return finalizeResponse(resp, req, target, idx), nil
		}

		lastErr = normalizeProviderError(err, target.Provider, target.Model)
		r.onTargetFailure(target, lastErr)
		if streamedAny {
			return nil, lastErr
		}

		resp, err = provider.Chat(ctx, attemptReq)
		if err == nil {
			r.onTargetSuccess(target)
			if onChunk != nil && resp != nil && resp.Content != "" {
				onChunk(resp.Content)
			}
			return finalizeResponse(resp, req, target, idx), nil
		}

		lastErr = normalizeProviderError(err, target.Provider, target.Model)
		r.onTargetFailure(target, lastErr)
		if !shouldContinueFallback(lastErr) {
			return nil, lastErr
		}
	}

	if lastErr == nil {
		lastErr = errors.New("no available llm provider")
	}
	return nil, lastErr
}

func (r *Router) resolveTargets(req *ChatRequest) []RouteTarget {
	if r == nil || req == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	var keys []string
	if capability := strings.TrimSpace(req.Capability); capability != "" {
		keys = append(keys, CapabilityRouteKey(capability))
	}
	if model := strings.TrimSpace(req.Model); model != "" {
		keys = append(keys, model)
	}
	keys = append(keys, r.fallback...)

	var out []RouteTarget
	seen := map[string]struct{}{}
	for _, key := range keys {
		for _, target := range r.routes[strings.TrimSpace(key)] {
			id := target.Provider + "::" + target.Model
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, target)
		}
	}
	return out
}

func (r *Router) getProvider(name string) (Provider, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p, ok := r.providers[strings.TrimSpace(name)]
	return p, ok
}

func (r *Router) allowTarget(target RouteTarget) bool {
	if r == nil || r.breaker == nil {
		return true
	}
	return r.breaker.Allow(target.Provider, target.Model)
}

func (r *Router) onTargetSuccess(target RouteTarget) {
	if r == nil || r.breaker == nil {
		return
	}
	r.breaker.MarkSuccess(target.Provider, target.Model)
}

func (r *Router) onTargetFailure(target RouteTarget, err error) {
	if r == nil || r.breaker == nil {
		return
	}
	r.breaker.MarkFailure(target.Provider, target.Model, providerErrorCategory(err))
}

func normalizeRouteTargets(targets []RouteTarget) []RouteTarget {
	out := make([]RouteTarget, 0, len(targets))
	seen := map[string]struct{}{}
	for _, target := range targets {
		target.Provider = strings.TrimSpace(target.Provider)
		target.Model = strings.TrimSpace(target.Model)
		if target.Provider == "" || target.Model == "" {
			continue
		}

		id := target.Provider + "::" + target.Model
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, target)
	}
	return out
}

func withRouteTarget(req *ChatRequest, target RouteTarget) *ChatRequest {
	if req == nil {
		return &ChatRequest{
			Model: target.Model,
		}
	}

	cloned := *req
	cloned.Model = target.Model
	return &cloned
}

func finalizeResponse(resp *ChatResponse, req *ChatRequest, target RouteTarget, fallbackHops int) *ChatResponse {
	if resp == nil {
		return nil
	}
	if strings.TrimSpace(resp.Model) == "" {
		resp.Model = target.Model
	}
	if strings.TrimSpace(resp.Provider) == "" {
		resp.Provider = target.Provider
	}
	if req != nil {
		resp.RequestedModel = req.Model
		resp.RequestedCapability = req.Capability
	}
	resp.FinalProvider = resp.Provider
	resp.FinalModel = resp.Model
	resp.FallbackHops = fallbackHops
	return resp
}
