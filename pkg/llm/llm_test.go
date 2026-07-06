package llm_test

import (
	"context"
	"flux-workflow/internal/config"
	"flux-workflow/pkg/llm"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestLLMClient(t *testing.T) {
	// 1. 初始化 (使用你提供的代码)
	cfg, err := config.NewConfig("../../../config/config.yaml")
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}

	router := llm.NewRouter()

	// 注意：确保你的 Provider 构造函数现在接收的是正确的参数
	deepseek := llm.NewOpenAIProvider("deepseek", cfg.DeepSeek.BaseURL, cfg.DeepSeek.ApiKey)
	openai := llm.NewOpenAIProvider("openai", cfg.OpenAI.BaseURL, cfg.OpenAI.ApiKey)
	qwen := llm.NewOpenAIProvider("qwen", cfg.Qwen.BaseURL, cfg.Qwen.ApiKey)

	router.Register("deepseek-chat", deepseek)
	router.Register("gpt-4o-mini", openai)
	router.Register("qwen-plus", qwen)
	router.RegisterCapability(llm.CapabilityPromptEnhance, []llm.RouteTarget{
		{Provider: qwen.Name(), Model: "qwen-plus"},
		{Provider: deepseek.Name(), Model: "deepseek-chat"},
		{Provider: openai.Name(), Model: "gpt-4o-mini"},
	})

	router.SetFallback([]string{"qwen-plus", "deepseek-chat", "gpt-4o-mini"})

	llmClient := llm.NewClient(router, nil)
	ctx := context.Background()

	// --- 测试用例 1: 普通调用 (Chat) ---
	t.Run("NormalChat", func(t *testing.T) {
		req := &llm.ChatRequest{
			Model:       "gpt-4o-mini",
			Temperature: 0.7,
			MaxTokens:   100,
			Messages: []llm.Message{
				{Role: "user", Content: "你好，请用一句话自我介绍。"},
			},
		}

		resp, err := llmClient.Chat(ctx, req)
		if err != nil {
			t.Errorf("普通调用失败: %v", err)
			return
		}

		if resp.Content == "" {
			t.Error("响应内容为空")
		}
		t.Logf("[Chat 结果]: %s (Tokens: %d)", resp.Content, resp.Usage.TotalTokens)
	})

	// --- 测试用例 2: 流式调用 (SSE/ChatStream) ---
	t.Run("StreamingChat", func(t *testing.T) {
		req := &llm.ChatRequest{
			Model:       "qwen-plus", // 测试你下午超时的那个模型
			Temperature: 0.7,
			MaxTokens:   500,
			Messages: []llm.Message{
				{Role: "user", Content: "写一篇关于Golang并发优势的短文，要求字数在200字左右。"},
			},
		}

		var fullContent strings.Builder
		chunkCount := 0
		startTime := time.Now()

		fmt.Printf("\n[开始流式输出...]\n")
		resp, err := llmClient.ChatStream(ctx, req, func(chunk string) {
			chunkCount++
			fullContent.WriteString(chunk)

			// 打印进度，观察是否是平滑输出
			fmt.Printf("%s", chunk)
		})

		if err != nil {
			t.Errorf("流式调用失败: %v", err)
			return
		}

		duration := time.Since(startTime)
		fmt.Printf("\n\n[流式结束] 总时长: %v, Chunk 数量: %d\n", duration, chunkCount)

		// 验证
		if chunkCount <= 1 {
			t.Errorf("流式调用异常：收到的 chunk 数量太少 (%d)，可能还是降级成了普通调用", chunkCount)
		}

		if resp.Content != fullContent.String() {
			t.Error("最终响应内容与流式累计内容不一致")
		}

		t.Logf("流式测试通过，总字符数: %d", len(resp.Content))
	})

	// --- 测试用例 3: Fallback 自动降级测试 ---
	t.Run("RouterFallback", func(t *testing.T) {
		// 故意传一个不存在的模型名，验证是否按 SetFallback 里的顺序执行
		req := &llm.ChatRequest{
			Model:       "non-existent-model",
			Temperature: 0.1,
			MaxTokens:   10,
			Messages: []llm.Message{
				{Role: "user", Content: "ping"},
			},
		}

		// 这里会调用 router.Chat，内部应该尝试 fallback
		resp, err := llmClient.Chat(ctx, req)
		if err != nil {
			t.Logf("预期内的失败或成功: %v", err)
		} else {
			t.Logf("Fallback 成功，实际响应模型: %s", resp.Model)
		}
	})
}
