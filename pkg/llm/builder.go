package llm

import (
	"github.com/tuxi/flux-workflow/internal/config"

	"github.com/redis/go-redis/v9"
)

type RouterOption func(*routerConfig)

type routerConfig struct {
	breakerStore  BreakerStateStore
	breakerConfig BreakerConfig
}

func defaultRouterConfig() routerConfig {
	return routerConfig{
		breakerStore:  NewMemoryBreakerStore(),
		breakerConfig: DefaultBreakerConfig(),
	}
}

func WithBreakerStore(store BreakerStateStore) RouterOption {
	return func(cfg *routerConfig) {
		if store != nil {
			cfg.breakerStore = store
		}
	}
}

func WithBreakerConfig(config BreakerConfig) RouterOption {
	return func(cfg *routerConfig) {
		if config.FailureLimit > 0 {
			cfg.breakerConfig = config
		}
	}
}

func NewRouter(options ...RouterOption) *Router {
	cfg := defaultRouterConfig()
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}

	return &Router{
		providers: map[string]Provider{},
		routes:    map[string][]RouteTarget{},
		breaker:   newSimpleBreaker(cfg.breakerStore, cfg.breakerConfig),
	}
}

func BuildDefaultRouter(cfg *config.Config, options ...RouterOption) *Router {
	router := NewRouter(options...)

	deepseek := NewOpenAIProvider("deepseek", cfg.DeepSeek.BaseURL, cfg.DeepSeek.ApiKey)
	openai := NewOpenAIProvider("openai", cfg.OpenAI.BaseURL, cfg.OpenAI.ApiKey)
	qwen := NewOpenAIProvider("qwen", cfg.Qwen.BaseURL, cfg.Qwen.ApiKey)

	router.Register("deepseek-chat", deepseek)
	router.Register("gpt-4o-mini", openai)
	router.Register("qwen-plus", qwen)

	defaultTargets := []RouteTarget{
		{Provider: qwen.Name(), Model: "qwen-plus"},
		{Provider: deepseek.Name(), Model: "deepseek-chat"},
		//{Provider: openai.Name(), Model: "gpt-4o-mini"},
	}

	router.RegisterCapability(CapabilityPromptEnhance, defaultTargets)
	router.RegisterCapability(CapabilityIntentExtraction, defaultTargets)
	router.RegisterCapability(CapabilityCreativeGenerate, defaultTargets)
	router.RegisterCapability(CapabilityPlanGenerate, defaultTargets)
	router.RegisterCapability(CapabilityVisionAggregation, defaultTargets)
	router.SetFallback([]string{"deepseek-chat", "gpt-4o-mini", "qwen-plus"})

	return router
}

func BuildDefaultClient(cfg *config.Config, multiCache MultiCache, options ...RouterOption) *Client {
	return NewClient(BuildDefaultRouter(cfg, options...), multiCache)
}

func BuildDefaultRedisClient(cfg *config.Config, redisClient *redis.Client) *Client {
	var options []RouterOption
	if redisClient != nil {
		options = append(options, WithBreakerStore(NewRedisBreakerStore(redisClient, "llm:breaker")))
	}

	var multiCache MultiCache
	if redisClient != nil {
		multiCache = NewRedisMultiCache(redisClient, 86400, 3)
	}

	return BuildDefaultClient(cfg, multiCache, options...)
}
