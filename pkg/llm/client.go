package llm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
)

type Client struct {
	router     *Router
	multiCache MultiCache // 多结果缓存
}

func NewClient(router *Router, multiCache MultiCache) *Client {
	return &Client{
		router:     router,
		multiCache: multiCache,
	}
}

func (c *Client) MultiCache() MultiCache {
	if c == nil {
		return nil
	}
	return c.multiCache
}

func (c *Client) Chat(
	ctx context.Context,
	req *ChatRequest,
) (*ChatResponse, error) {
	noCache := req != nil && req.NoCache
	cacheKey := c.buildCacheKey(req)
	// 1. 随机读一个缓存
	if !noCache && c.multiCache != nil {
		if s, ok := c.multiCache.GetRandom(cacheKey); ok {
			resp := &ChatResponse{
				Content:             s,
				RequestedModel:      requestedModel(req),
				RequestedCapability: requestedCapability(req),
			}
			c.logResponse("chat_cache_hit", resp)
			return resp, nil
		}
	}

	// 2. 真实请求 LLM
	resp, err := c.router.Chat(ctx, req)
	if err != nil {
		return nil, err
	}

	// 3. 写入多结果缓存（最多保留 3 个）
	if !noCache && c.multiCache != nil {
		c.multiCache.Append(cacheKey, resp.Content)
	}
	c.logResponse("chat_success", resp)

	return resp, nil
}

// StreamCallback 流式回调函数（客户端实时接收chunk
type StreamCallback func(chunk string)

// ChatStream 流式调用LLM
func (c *Client) ChatStream(ctx context.Context, req *ChatRequest, onChunk StreamCallback) (*ChatResponse, error) {
	cacheKey := c.buildCacheKey(req)
	if cacheKey != "" {
		if c.multiCache != nil {
			if s, ok := c.multiCache.GetRandom(cacheKey); ok {
				resp := &ChatResponse{
					Content:             s,
					RequestedModel:      requestedModel(req),
					RequestedCapability: requestedCapability(req),
				}
				c.logResponse("chat_stream_cache_hit", resp)
				if onChunk != nil {
					onChunk(s)
				}
				return resp, nil
			}
		}
	}

	resp, err := c.router.ChatStream(ctx, req, onChunk)
	if err != nil {
		return nil, err
	}

	if c.multiCache != nil {
		c.multiCache.Append(cacheKey, resp.Content) // 最多3条
	}
	c.logResponse("chat_stream_success", resp)

	return resp, nil
}

func (c *Client) logResponse(event string, resp *ChatResponse) {
	if resp == nil {
		return
	}
	log.Printf(
		"llm %s requested_model=%q requested_capability=%q final_provider=%q final_model=%q fallback_hops=%d",
		event,
		resp.RequestedModel,
		resp.RequestedCapability,
		resp.FinalProvider,
		resp.FinalModel,
		resp.FallbackHops,
	)
}

func requestedModel(req *ChatRequest) string {
	if req == nil {
		return ""
	}
	return req.Model
}

func requestedCapability(req *ChatRequest) string {
	if req == nil {
		return ""
	}
	return req.Capability
}

// buildCacheKey 生成缓存 key。
// 优先使用显式传入的 CacheKey，这样调用方可以按稳定的业务语义命中缓存；
// 若未传 CacheKey，则回退为基于 Model + Messages 的默认哈希策略。
func (c *Client) buildCacheKey(req *ChatRequest) string {
	if req != nil && req.CacheKey != "" {
		type explicitCacheStruct struct {
			Model      string `json:"model,omitempty"`
			Capability string `json:"capability,omitempty"`
			CacheKey   string `json:"cache_key"`
		}

		bs, _ := json.Marshal(explicitCacheStruct{
			Model:      req.Model,
			Capability: req.Capability,
			CacheKey:   req.CacheKey,
		})
		hash := sha256.Sum256(bs)
		return "llm:" + hex.EncodeToString(hash[:])
	}

	type cacheStruct struct {
		Model      string    `json:"model,omitempty"`
		Capability string    `json:"capability,omitempty"`
		Messages   []Message `json:"messages"`
	}

	hashStruct := cacheStruct{
		Model:      req.Model,
		Capability: req.Capability,
		Messages:   req.Messages,
	}

	bs, _ := json.Marshal(hashStruct)
	hash := sha256.Sum256(bs)
	return "llm:" + hex.EncodeToString(hash[:])
}
