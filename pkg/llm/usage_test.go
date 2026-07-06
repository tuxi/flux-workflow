package llm

import "testing"

func TestBuildUsageFacts(t *testing.T) {
	facts := BuildUsageFacts(map[string]any{
		"llm_provider":      "qwen",
		"llm_model":         "qwen-plus",
		"prompt_tokens":     321,
		"completion_tokens": 87,
		"total_tokens":      408,
		"llm_fallback_hops": 1,
	})
	if len(facts) != 1 {
		t.Fatalf("expected 1 fact, got %#v", facts)
	}
	if facts[0]["resource_type"] != "llm" {
		t.Fatalf("unexpected resource type: %#v", facts[0])
	}
	if facts[0]["usage_quantity"] != float64(408) {
		t.Fatalf("unexpected usage quantity: %#v", facts[0])
	}
	breakdown, ok := facts[0]["usage_breakdown"].(map[string]any)
	if !ok {
		t.Fatalf("missing usage breakdown: %#v", facts[0])
	}
	if breakdown["prompt_tokens"] != 321 {
		t.Fatalf("unexpected breakdown: %#v", breakdown)
	}
}
