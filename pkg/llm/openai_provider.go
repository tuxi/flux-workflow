package llm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/sashabaranov/go-openai"
)

type OpenAIProvider struct {
	name    string
	apiKey  string
	baseURL string
}

func NewOpenAIProvider(
	name string,
	baseURL string,
	apiKey string,
) *OpenAIProvider {
	return &OpenAIProvider{
		name:    name,
		baseURL: baseURL,
		apiKey:  apiKey,
	}
}

func (o *OpenAIProvider) Name() string {
	return o.name
}

// 获取公用的 SDK Client 配置
func (o *OpenAIProvider) getClient() *openai.Client {
	config := openai.DefaultConfig(o.apiKey)
	config.BaseURL = o.baseURL
	return openai.NewClientWithConfig(config)
}

// Chat 普通非流式调用
func (o *OpenAIProvider) Chat(
	ctx context.Context,
	req *ChatRequest,
) (*ChatResponse, error) {
	client := o.getClient()

	chatReq := openai.ChatCompletionRequest{
		Model:       req.Model,
		Messages:    o.convertMessages(req.Messages),
		Temperature: float32(req.Temperature),
		// 针对普通模型，建议仍优先使用 MaxTokens 或两者都传
		MaxTokens:           req.MaxTokens,
		MaxCompletionTokens: req.MaxTokens,
	}
	// JSON 模式：强制模型只输出 JSON object，避免夹带解释性文本导致解析失败。
	if req.JSONMode {
		chatReq.ResponseFormat = &openai.ChatCompletionResponseFormat{
			Type: openai.ChatCompletionResponseFormatTypeJSONObject,
		}
	}

	resp, err := client.CreateChatCompletion(ctx, chatReq)

	if err != nil {
		return nil, classifyOpenAIError(o.name, req.Model, fmt.Errorf("openai chat error: %w", err))
	}

	if len(resp.Choices) == 0 {
		return nil, errors.New("empty choices in response")
	}

	return &ChatResponse{
		Content: resp.Choices[0].Message.Content,
		Model:   resp.Model,
		Usage: Usage{
			PromptTokens:     resp.Usage.PromptTokens,
			CompletionTokens: resp.Usage.CompletionTokens,
			TotalTokens:      resp.Usage.TotalTokens,
		},
	}, nil
}

// ChatStream 流式调用
func (o *OpenAIProvider) ChatStream(
	ctx context.Context,
	req *ChatRequest,
	onChunk StreamCallback,
) (*ChatResponse, error) {
	client := o.getClient()

	// 构造流式请求
	streamReq := openai.ChatCompletionRequest{
		Model:       req.Model,
		Messages:    o.convertMessages(req.Messages),
		Temperature: float32(req.Temperature),
		Stream:      true,
	}

	// 智能判断字段：如果是 o1 系列使用 MaxCompletionTokens
	if strings.HasPrefix(req.Model, "o1") || strings.HasPrefix(req.Model, "o3") {
		streamReq.MaxCompletionTokens = req.MaxTokens
	} else {
		streamReq.MaxTokens = req.MaxTokens
	}

	// 如果需要流式返回 Usage 统计，需要开启此选项（OpenAI 原厂支持）
	streamReq.StreamOptions = &openai.StreamOptions{
		IncludeUsage: true,
	}

	stream, err := client.CreateChatCompletionStream(ctx, streamReq)
	if err != nil {
		return nil, classifyOpenAIError(o.name, req.Model, err)
	}
	defer stream.Close()

	var fullContent strings.Builder
	var finalUsage Usage

	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, classifyOpenAIError(o.name, req.Model, fmt.Errorf("stream recv error: %w", err))
		}

		// 处理内容碎片
		if len(response.Choices) > 0 {
			content := response.Choices[0].Delta.Content
			if content != "" {
				fullContent.WriteString(content)
				if onChunk != nil {
					onChunk(content)
				}
			}
		}

		// 获取 Usage 信息 (通常在流的最后一个 chunk 返回)
		if response.Usage != nil {
			finalUsage = Usage{
				PromptTokens:     response.Usage.PromptTokens,
				CompletionTokens: response.Usage.CompletionTokens,
				TotalTokens:      response.Usage.TotalTokens,
			}
		}
	}

	return &ChatResponse{
		Content: fullContent.String(),
		Model:   req.Model,
		Usage:   finalUsage,
	}, nil
}

// convertMessages 实现具体的类型转换逻辑
func (o *OpenAIProvider) convertMessages(msgs []Message) []openai.ChatCompletionMessage {
	converted := make([]openai.ChatCompletionMessage, len(msgs))
	for i, m := range msgs {
		converted[i] = openai.ChatCompletionMessage{
			Role:    m.Role,
			Content: m.Content,
		}
	}
	return converted
}

func classifyOpenAIError(providerName, model string, err error) error {
	if err == nil {
		return nil
	}

	var apiErr *openai.APIError
	if errors.As(err, &apiErr) {
		category := ErrorCategoryUnknown
		code := strings.ToLower(fmt.Sprint(apiErr.Code))
		message := strings.ToLower(apiErr.Message)

		switch apiErr.HTTPStatusCode {
		case 400, 404:
			category = ErrorCategoryBadRequest
		case 401, 403:
			if strings.Contains(message, "quota") || strings.Contains(message, "insufficient") || strings.Contains(code, "quota") {
				category = ErrorCategoryQuota
			} else {
				category = ErrorCategoryAuth
			}
		case 402:
			category = ErrorCategoryQuota
		case 408:
			category = ErrorCategoryTimeout
		case 429:
			if strings.Contains(message, "quota") || strings.Contains(message, "insufficient") || strings.Contains(code, "quota") {
				category = ErrorCategoryQuota
			} else {
				category = ErrorCategoryRateLimit
			}
		default:
			if apiErr.HTTPStatusCode >= 500 {
				category = ErrorCategoryUnavailable
			}
		}

		return newProviderError(providerName, model, category, err)
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return newProviderError(providerName, model, ErrorCategoryTimeout, err)
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return newProviderError(providerName, model, ErrorCategoryTimeout, err)
	}

	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "insufficient_quota"),
		strings.Contains(lower, "quota exceeded"),
		strings.Contains(lower, "balance"),
		strings.Contains(lower, "billing"),
		strings.Contains(lower, "credit"):
		return newProviderError(providerName, model, ErrorCategoryQuota, err)
	case strings.Contains(lower, "rate limit"),
		strings.Contains(lower, "too many requests"):
		return newProviderError(providerName, model, ErrorCategoryRateLimit, err)
	case strings.Contains(lower, "timeout"),
		strings.Contains(lower, "deadline exceeded"):
		return newProviderError(providerName, model, ErrorCategoryTimeout, err)
	}

	return newProviderError(providerName, model, ErrorCategoryUnknown, err)
}
