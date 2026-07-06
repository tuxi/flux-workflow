package llm

import (
	"log"
	"time"
)

type BreakerConfig struct {
	FailureLimit   int
	RecoverAfter   time.Duration
	RateLimitFor   time.Duration
	AuthOpenFor    time.Duration
	QuotaOpenFor   time.Duration
	TimeoutOpenFor time.Duration
}

type breakerState struct {
	Failures  int       `json:"failures"`
	OpenUntil time.Time `json:"open_until"`
}

type BreakerStateStore interface {
	Load(key string) (breakerState, bool, error)
	Save(key string, state breakerState, ttl time.Duration) error
	Delete(key string) error
}

type simpleBreaker struct {
	store BreakerStateStore
	cfg   BreakerConfig
}

func DefaultBreakerConfig() BreakerConfig {
	return BreakerConfig{
		FailureLimit:   2,
		RecoverAfter:   30 * time.Second,
		RateLimitFor:   20 * time.Second,
		AuthOpenFor:    10 * time.Minute,
		QuotaOpenFor:   15 * time.Minute,
		TimeoutOpenFor: 45 * time.Second,
	}
}

func newSimpleBreaker(store BreakerStateStore, cfg BreakerConfig) *simpleBreaker {
	if store == nil {
		store = NewMemoryBreakerStore()
	}
	if cfg.FailureLimit <= 0 {
		cfg = DefaultBreakerConfig()
	}
	return &simpleBreaker{
		store: store,
		cfg:   cfg,
	}
}

func (b *simpleBreaker) Allow(provider, model string) bool {
	if b == nil {
		return true
	}

	state, ok, err := b.store.Load(breakerKey(provider, model))
	if err != nil {
		log.Printf("llm breaker load failed: provider=%s model=%s err=%v", provider, model, err)
		return true
	}
	if !ok {
		return true
	}
	if state.OpenUntil.After(time.Now()) {
		return false
	}
	if err := b.store.Delete(breakerKey(provider, model)); err != nil {
		log.Printf("llm breaker delete failed: provider=%s model=%s err=%v", provider, model, err)
	}
	return true
}

func (b *simpleBreaker) MarkSuccess(provider, model string) {
	if b == nil {
		return
	}
	if err := b.store.Delete(breakerKey(provider, model)); err != nil {
		log.Printf("llm breaker mark success delete failed: provider=%s model=%s err=%v", provider, model, err)
	}
}

func (b *simpleBreaker) MarkFailure(provider, model string, category ErrorCategory) {
	if b == nil {
		return
	}

	key := breakerKey(provider, model)
	state, _, err := b.store.Load(key)
	if err != nil {
		log.Printf("llm breaker load before mark failure failed: provider=%s model=%s err=%v", provider, model, err)
		state = breakerState{}
	}

	state.Failures++
	now := time.Now()

	switch category {
	case ErrorCategoryAuth:
		state.OpenUntil = now.Add(b.cfg.AuthOpenFor)
	case ErrorCategoryQuota:
		state.OpenUntil = now.Add(b.cfg.QuotaOpenFor)
	case ErrorCategoryRateLimit:
		state.OpenUntil = now.Add(b.cfg.RateLimitFor)
	case ErrorCategoryTimeout:
		if state.Failures >= b.cfg.FailureLimit {
			state.OpenUntil = now.Add(b.cfg.TimeoutOpenFor)
		}
	case ErrorCategoryUnavailable:
		if state.Failures >= b.cfg.FailureLimit {
			state.OpenUntil = now.Add(b.cfg.RecoverAfter)
		}
	}

	ttl := b.stateTTL(category, state, now)
	if err := b.store.Save(key, state, ttl); err != nil {
		log.Printf("llm breaker save failed: provider=%s model=%s category=%s err=%v", provider, model, category, err)
	}
}

func (b *simpleBreaker) stateTTL(category ErrorCategory, state breakerState, now time.Time) time.Duration {
	if state.OpenUntil.After(now) {
		return time.Until(state.OpenUntil) + time.Minute
	}

	switch category {
	case ErrorCategoryRateLimit:
		return b.cfg.RateLimitFor + time.Minute
	case ErrorCategoryTimeout:
		return b.cfg.TimeoutOpenFor + time.Minute
	case ErrorCategoryUnavailable, ErrorCategoryUnknown:
		return b.cfg.RecoverAfter + time.Minute
	default:
		return b.cfg.RecoverAfter + time.Minute
	}
}

func breakerKey(provider, model string) string {
	return provider + "::" + model
}
