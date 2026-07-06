package service

import (
	"context"
	"github.com/tuxi/flux-workflow/domain"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreativeDetailService_SignCreativeDetailURLs(t *testing.T) {
	svc := &creativeDetailService{assetSigner: fakeCreativeDetailURLSigner{}}
	detail := &domain.CreativeDetail{
		Version:      "v1",
		WorkflowName: "image_to_image",
		Mode:         "image_to_image",
		Input: domain.CreativeDetailBlock{
			Title: "用户输入",
			Sections: []domain.CreativeDetailSection{
				{
					Key:   "source_media",
					Title: "源图",
					Type:  domain.CreativeSectionTypeMedia,
					Payload: domain.MediaPayload{
						Type:       "image",
						URL:        "https://dreamlog.oss-cn-beijing.aliyuncs.com/prod/private/user-upload/source.jpg",
						PreviewURL: "https://dreamlog.oss-cn-beijing.aliyuncs.com/prod/private/user-upload/source.jpg",
					},
				},
			},
		},
		Output: domain.CreativeDetailBlock{
			Title: "生成结果",
			Sections: []domain.CreativeDetailSection{
				{
					Key:   "result_media",
					Title: "结果资源",
					Type:  domain.CreativeSectionTypeMedia,
					Payload: domain.MediaPayload{
						Type:       "image",
						URL:        "https://dreamlog.oss-cn-beijing.aliyuncs.com/images/final/result.jpeg",
						PreviewURL: "https://dreamlog.oss-cn-beijing.aliyuncs.com/images/final/result.jpeg",
					},
				},
				{
					Key:   "debug_payload",
					Title: "嵌套数据",
					Type:  domain.CreativeSectionTypeCard,
					Payload: map[string]any{
						"nested": map[string]any{
							"reference_image": "https://dreamlog.oss-cn-beijing.aliyuncs.com/images/reference/ref.jpeg",
						},
					},
				},
			},
		},
	}

	signed := svc.signCreativeDetailURLs(context.Background(), 1001, detail)

	require.NotNil(t, signed)
	sourcePayload := signed.Input.Sections[0].Payload.(map[string]any)
	requireSignedURL(t, sourcePayload["url"])
	requireSignedURL(t, sourcePayload["preview_url"])

	resultPayload := signed.Output.Sections[0].Payload.(map[string]any)
	requireSignedURL(t, resultPayload["url"])
	requireSignedURL(t, resultPayload["preview_url"])

	debugPayload := signed.Output.Sections[1].Payload.(map[string]any)
	nested := debugPayload["nested"].(map[string]any)
	requireSignedURL(t, nested["reference_image"])
}

func requireSignedURL(t *testing.T, value any) {
	t.Helper()
	s, ok := value.(string)
	require.True(t, ok)
	require.Contains(t, s, "Expires=1800")
}

type fakeCreativeDetailURLSigner struct{}

func (fakeCreativeDetailURLSigner) SignURLsInValue(ctx context.Context, userID int64, value any) any {
	_ = ctx
	_ = userID
	return fakeSignCreativeDetailValue(value)
}

func (fakeCreativeDetailURLSigner) HydrateAssetRefs(_ context.Context, _ int64, value any) any {
	return value
}

func fakeSignCreativeDetailValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = fakeSignCreativeDetailValue(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = fakeSignCreativeDetailValue(item)
		}
		return out
	case string:
		if strings.HasPrefix(v, "https://dreamlog.oss-cn-beijing.aliyuncs.com/") && !strings.Contains(v, "Expires=") {
			return v + "?Expires=1800"
		}
		return v
	default:
		return value
	}
}
