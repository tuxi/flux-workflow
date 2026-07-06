package llm

import (
	"errors"
	"strings"
)

type ErrorCategory string

const (
	ErrorCategoryUnknown     ErrorCategory = "unknown"
	ErrorCategoryAuth        ErrorCategory = "auth"
	ErrorCategoryQuota       ErrorCategory = "quota"
	ErrorCategoryRateLimit   ErrorCategory = "rate_limit"
	ErrorCategoryTimeout     ErrorCategory = "timeout"
	ErrorCategoryBadRequest  ErrorCategory = "bad_request"
	ErrorCategoryUnavailable ErrorCategory = "unavailable"
)

var ErrCircuitOpen = errors.New("llm circuit breaker is open")

type ProviderError struct {
	Provider string
	Model    string
	Category ErrorCategory
	Err      error
}

func (e *ProviderError) Error() string {
	if e == nil {
		return ""
	}

	parts := []string{"llm error"}
	if e.Provider != "" {
		parts = append(parts, "provider="+e.Provider)
	}
	if e.Model != "" {
		parts = append(parts, "model="+e.Model)
	}
	if e.Category != "" {
		parts = append(parts, "category="+string(e.Category))
	}
	if e.Err != nil {
		parts = append(parts, "err="+e.Err.Error())
	}
	return strings.Join(parts, " ")
}

func (e *ProviderError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func shouldContinueFallback(err error) bool {
	switch providerErrorCategory(err) {
	case ErrorCategoryAuth,
		ErrorCategoryQuota,
		ErrorCategoryRateLimit,
		ErrorCategoryTimeout,
		ErrorCategoryUnavailable,
		ErrorCategoryUnknown:
		return true
	default:
		return false
	}
}

func providerErrorCategory(err error) ErrorCategory {
	var providerErr *ProviderError
	if errors.As(err, &providerErr) && providerErr.Category != "" {
		return providerErr.Category
	}
	return ErrorCategoryUnknown
}

func normalizeProviderError(err error, provider, model string) error {
	if err == nil {
		return nil
	}

	var providerErr *ProviderError
	if errors.As(err, &providerErr) {
		if providerErr.Provider == "" {
			providerErr.Provider = provider
		}
		if providerErr.Model == "" {
			providerErr.Model = model
		}
		if providerErr.Category == "" {
			providerErr.Category = ErrorCategoryUnknown
		}
		return providerErr
	}

	return &ProviderError{
		Provider: provider,
		Model:    model,
		Category: ErrorCategoryUnknown,
		Err:      err,
	}
}

func newProviderError(provider, model string, category ErrorCategory, err error) error {
	if err == nil {
		err = errors.New("unknown provider error")
	}
	return &ProviderError{
		Provider: provider,
		Model:    model,
		Category: category,
		Err:      err,
	}
}
