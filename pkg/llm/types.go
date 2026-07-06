package llm

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ChatRequest struct {
	Model      string
	Capability string
	Messages   []Message
	// CacheKey 可选。
	// 传入时，LLM 客户端优先基于该业务语义 key 构建缓存键，
	// 适用于 prompt 文本会波动、但业务语义相同的场景。
	// 为空时，回退到基于 Model + Messages 的默认缓存策略。
	CacheKey    string
	Temperature float64
	MaxTokens   int
	// JSONMode 为 true 时，请求 provider 以 JSON object 模式输出，
	// 避免模型在 JSON 前后夹带解释性文本导致解析失败。
	JSONMode bool
	// NoCache 为 true 时，跳过多结果缓存的读写，强制每次实时生成。
	// 适用于希望保证多样性的场景（如无任何输入时的灵感生成）。
	NoCache bool
}

type Usage struct {
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
}

type ChatResponse struct {
	Content             string
	Model               string
	Provider            string
	RequestedModel      string
	RequestedCapability string
	FinalProvider       string
	FinalModel          string
	FallbackHops        int
	Usage               Usage
}
