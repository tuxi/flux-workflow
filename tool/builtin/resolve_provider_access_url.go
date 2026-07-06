package builtin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/tuxi/flux/tool"
)

// AssetProviderResolver generates a signed URL that a third-party AI provider can access.
// The URL must be generated at node execution time, not at task creation time.
type AssetProviderResolver interface {
	// ResolveForProvider returns a signed URL valid for expireSeconds from now.
	// userID=0 is allowed for admin-catalog and system assets.
	ResolveForProvider(ctx context.Context, userID int64, assetID int64, expireSeconds int64) (providerURL string, ossKey string, expiresAt *time.Time, err error)
}

// ResolveProviderAccessURLTool converts an asset_id into a provider-accessible signed URL
// at node execution time. Avoids downloading and re-uploading private assets.
//
// Priority: asset_id > oss_key > url (external, passed through as-is).
type ResolveProviderAccessURLTool struct {
	resolver AssetProviderResolver
}

func NewResolveProviderAccessURLTool(resolver AssetProviderResolver) *ResolveProviderAccessURLTool {
	return &ResolveProviderAccessURLTool{resolver: resolver}
}

func (t *ResolveProviderAccessURLTool) Name() string {
	return "resolve_provider_access_url"
}

func (t *ResolveProviderAccessURLTool) Description() string {
	return "将 asset_id 在节点执行时解析为服务商可访问的签名 URL，无需下载再上传"
}

func (t *ResolveProviderAccessURLTool) Mode() tool.ExecutionMode {
	return tool.SyncExecution
}

func (t *ResolveProviderAccessURLTool) InputSchema() tool.DataSchema {
	return tool.DataSchema{Fields: map[string]tool.FieldSchema{
		"asset_id":       {Type: "number", Required: false, Desc: "资产台账 ID，优先使用"},
		"oss_key":        {Type: "string", Required: false, Desc: "OSS Key，asset_id 缺失时使用"},
		"url":            {Type: "string", Required: false, Desc: "外部 URL，无法解析时透传"},
		"slot":           {Type: "string", Required: false, Desc: "资源槽位名（透传）"},
		"asset_kind":     {Type: "string", Required: false, Desc: "资源类型（透传）"},
		"source":         {Type: "string", Required: false, Desc: "来源（透传）"},
		"expire_seconds": {Type: "number", Required: false, Desc: "签名 URL 有效期（秒），默认 7200"},
		"required":       {Type: "bool", Required: false, Desc: "是否必须，默认 true"},
	}}
}

func (t *ResolveProviderAccessURLTool) OutputSchema() tool.DataSchema {
	return tool.DataSchema{Fields: map[string]tool.FieldSchema{
		"provider_url": {Type: "string", Desc: "服务商可访问 URL"},
		"url":          {Type: "string", Desc: "同 provider_url"},
		"expires_at":   {Type: "string", Desc: "签名过期时间（RFC3339）"},
		"asset_id":     {Type: "number", Desc: "原始资产 ID"},
		"oss_key":      {Type: "string", Desc: "OSS Key"},
		"slot":         {Type: "string", Desc: "透传槽位名"},
		"asset_kind":   {Type: "string", Desc: "透传资源类型"},
		"resolve_mode": {Type: "string", Desc: "signed_url / external_url"},
	}}
}

func (t *ResolveProviderAccessURLTool) Execute(ctx context.Context, input map[string]any, emitter tool.ToolEmitter) (*tool.Result, error) {
	allowFailure := false
	if v, ok := input["allow_failure"].(bool); ok {
		allowFailure = v
	}
	required := true
	if v, ok := input["required"].(bool); ok {
		required = v
	}
	if !required {
		allowFailure = true
	}

	assetID := int64FromAny(input["asset_id"])
	ossKey := strings.TrimSpace(toString(input["oss_key"]))
	rawURL := strings.TrimSpace(toString(input["url"]))
	slot := strings.TrimSpace(toString(input["slot"]))
	assetKind := strings.TrimSpace(toString(input["asset_kind"]))
	expireSeconds := int64FromAny(input["expire_seconds"])
	if expireSeconds <= 0 {
		expireSeconds = 7200
	}

	userID := int64(0)
	if meta, ok := tool.ExecutionMetaFromContext(ctx); ok {
		userID = meta.UserID
	}

	// asset_id path: generate signed URL at execution time.
	if assetID > 0 && t.resolver != nil {
		tool.EmitStart(emitter, "解析资产签名 URL", map[string]any{"asset_id": assetID, "slot": slot})
		providerURL, resolvedKey, expiresAt, err := t.resolver.ResolveForProvider(ctx, userID, assetID, expireSeconds)
		if err != nil {
			return t.fail(emitter, allowFailure, fmt.Errorf("resolve asset %d: %w", assetID, err), input)
		}
		out := t.successOut(providerURL, resolvedKey, assetID, slot, assetKind, expiresAt, "signed_url")
		tool.EmitComplete(emitter, "签名 URL 生成成功", map[string]any{"asset_id": assetID, "slot": slot})
		return tool.Success(out), nil
	}

	// External URL or already-accessible URL: pass through.
	if rawURL != "" {
		tool.EmitComplete(emitter, "外部 URL 透传", map[string]any{"url": rawURL, "slot": slot})
		out := t.successOut(rawURL, ossKey, assetID, slot, assetKind, nil, "external_url")
		return tool.Success(out), nil
	}

	err := fmt.Errorf("asset_id, oss_key 和 url 均为空，无法解析服务商资源")
	return t.fail(emitter, allowFailure, err, input)
}

func (t *ResolveProviderAccessURLTool) successOut(providerURL, ossKey string, assetID int64, slot, assetKind string, expiresAt *time.Time, mode string) map[string]any {
	out := map[string]any{
		"provider_url": providerURL,
		"url":          providerURL,
		"asset_id":     assetID,
		"oss_key":      ossKey,
		"slot":         slot,
		"asset_kind":   assetKind,
		"resolve_mode": mode,
	}
	if expiresAt != nil {
		out["expires_at"] = expiresAt.Format(time.RFC3339)
	}
	return out
}

func (t *ResolveProviderAccessURLTool) fail(emitter tool.ToolEmitter, allowFailure bool, err error, input map[string]any) (*tool.Result, error) {
	tool.EmitFail(emitter, err, input)
	if allowFailure {
		return &tool.Result{Success: true, Data: map[string]any{
			"provider_url": "",
			"url":          "",
			"resolve_mode": "failed",
		}}, nil
	}
	return nil, err
}

func toString(v any) string {
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}
