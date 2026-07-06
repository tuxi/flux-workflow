package builtin

import (
	"context"
	"flux-workflow/pkg/oss"
	"fmt"

	"github.com/tuxi/flux/tool"
)

// SingleUploadStorageTool 只支持单个资源上传的工具
type SingleUploadStorageTool struct {
	ossClient oss.Client
}

func NewSingleUploadStorageTool(ossClient oss.Client) *SingleUploadStorageTool {
	return &SingleUploadStorageTool{ossClient: ossClient}
}

func (s SingleUploadStorageTool) Name() string {
	return "single_upload_storage"
}

func (s SingleUploadStorageTool) Description() string {
	return "Upload file to Aliyun OSS and return accessible URL"
}

func (s SingleUploadStorageTool) InputSchema() tool.DataSchema {
	return tool.DataSchema{
		Fields: map[string]tool.FieldSchema{
			"file_path": {
				Type:     "string",
				Required: true,
				Desc:     "本地文件绝对路径",
			},
			"file_type": {
				Type:     "string",
				Required: false,
				Desc:     "文件类型",
			},
			"dir": {
				Type:     "string",
				Required: false,
				Desc:     "OSS目录，如 videos/final 或 videos/covers",
			},
			"file_name_prefix": {
				Type:     "string",
				Required: false,
				Desc:     "文件名前缀",
			},
			"allow_failure": {
				Type:     "bool",
				Required: false,
				Desc:     "失败时是否返回状态而不是中断工作流，默认 false",
			},
			"asset_class": {
				Type:     "string",
				Required: false,
				Desc:     "资产分类，如 task_result / task_intermediate",
			},
			"asset_kind": {
				Type:     "string",
				Required: false,
				Desc:     "资产类型，如 image / video / audio",
			},
			"visibility": {
				Type:     "string",
				Required: false,
				Desc:     "访问级别 public / private / internal",
			},
		},
	}
}

func (s SingleUploadStorageTool) OutputSchema() tool.DataSchema {
	return tool.DataSchema{
		Fields: map[string]tool.FieldSchema{
			"url": {
				Type: "string",
				Desc: "阿里云OSS文件访问URL",
			},
			"oss_key": {
				Type: "string",
				Desc: "OSS文件存储的Key（路径）",
			},
			"upload_status": {
				Type: "string",
				Desc: "success / failed",
			},
			"upload_failure_reason": {
				Type: "string",
				Desc: "上传失败原因",
			},
			"asset_id": {
				Type: "number",
				Desc: "资产台账ID",
			},
			"provider_url": {
				Type: "string",
				Desc: "供外部服务商拉取的访问URL，provider_access 资源优先使用",
			},
		},
	}
}

func (s SingleUploadStorageTool) Execute(ctx context.Context, input map[string]any, emitter tool.ToolEmitter) (*tool.Result, error) {
	allowFailure := false
	if v, ok := input["allow_failure"].(bool); ok {
		allowFailure = v
	}
	path, _ := input["file_path"].(string)

	if len(path) == 0 {
		tool.EmitFail(emitter, fmt.Errorf("无可用的上传文件，input.file_path 为空"), nil)
		if allowFailure {
			return &tool.Result{
				Success: true,
				Data: map[string]any{
					"url":                   "",
					"oss_key":               "",
					"upload_status":         "failed",
					"upload_failure_reason": "file_path is nil",
				},
			}, nil
		}
		return nil, fmt.Errorf("file_path is nil")
	}

	dir, _ := input["dir"].(string)
	fileNamePrefix, _ := input["file_name_prefix"].(string)
	if dir == "" {
		dir = s.Name()
	}
	assetClass, _ := input["asset_class"].(string)
	assetKind, _ := input["asset_kind"].(string)
	visibility, _ := input["visibility"].(string)
	businessType, _ := input["business_type"].(string)
	nodeName, _ := input["node_name"].(string)
	taskID := int64FromAny(input["task_id"])
	ownerID := int64FromAny(input["owner_id"])
	ownerType, _ := input["owner_type"].(string)
	if meta, ok := tool.ExecutionMetaFromContext(ctx); ok {
		if taskID <= 0 {
			taskID = meta.TaskID
		}
		if ownerID <= 0 {
			ownerID = meta.UserID
		}
		if nodeName == "" {
			nodeName = meta.NodeName
		}
	}
	if assetClass == "" {
		if dir == "videos/final" || dir == "videos/covers" || dir == "images/final" || dir == "visual_goods/final" || dir == "visual_goods/covers" {
			assetClass = "task_result"
		} else {
			assetClass = "task_intermediate"
		}
	}
	if ownerType == "" {
		ownerType = "user"
	}
	if visibility == "" {
		// single_upload_storage 的输出通常会进入节点输出或任务详情；默认私有短签展示，
		// 需要完全内部隐藏的中转文件由 DSL 显式传 visibility=internal。
		visibility = "private"
	}
	tool.EmitStart(emitter, "开始上传文件", map[string]any{
		"file": path,
	})

	res, err := s.ossClient.UploadWithOptions(path, oss.UploadOptions{
		CustomDir:      dir,
		CustomFilename: fileNamePrefix,
		AssetClass:     assetClass,
		AssetKind:      assetKind,
		Visibility:     visibility,
		Source:         "server_upload",
		BusinessType:   businessType,
		TaskID:         taskID,
		NodeName:       nodeName,
		OwnerType:      ownerType,
		OwnerID:        ownerID,
	})
	if err != nil {
		tool.EmitFail(emitter, fmt.Errorf("文件上传失败"), map[string]any{
			"path":  path,
			"dir":   dir,
			"error": err.Error(),
		})

		if allowFailure {
			return &tool.Result{
				Success: true,
				Data: map[string]any{
					"url":                   "",
					"oss_key":               "",
					"upload_status":         "failed",
					"upload_failure_reason": err.Error(),
				},
			}, nil
		}
		return nil, err
	}

	tool.EmitProgress(emitter, 1, nil)

	result := map[string]any{
		"url":           res.URL,
		"provider_url":  res.ProviderURL,
		"oss_key":       res.OSSKey,
		"file_size":     res.FileSize,
		"asset_id":      res.AssetID,
		"upload_status": "success",
	}
	tool.EmitComplete(emitter, "文件上传完成", result)

	return &tool.Result{Data: result}, nil
}

func (s SingleUploadStorageTool) Mode() tool.ExecutionMode {
	return tool.SyncExecution
}

func int64FromAny(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case float32:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		var out int64
		_, _ = fmt.Sscanf(n, "%d", &out)
		return out
	default:
		return 0
	}
}
