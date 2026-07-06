package builtin

import (
	"context"
	"flux-workflow/pkg/oss"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tuxi/flux/tool"
)

// SignalResourceDownloadTool 通用资源下载工具（仅支持单个视频/图片，增强重试）
type SignalResourceDownloadTool struct {
	ossClient oss.Client
}

func NewSignalResourceDownloadTool(ossClients ...oss.Client) *SignalResourceDownloadTool {
	var client oss.Client
	if len(ossClients) > 0 {
		client = ossClients[0]
	}
	return &SignalResourceDownloadTool{ossClient: client}
}

func (t *SignalResourceDownloadTool) Name() string {
	return "single_resource_download"
}

func (t *SignalResourceDownloadTool) Description() string {
	return "下载资源文件（图片/视频），支持重试"
}

func (t *SignalResourceDownloadTool) InputSchema() tool.DataSchema {
	return tool.DataSchema{
		Fields: map[string]tool.FieldSchema{
			"file_url": {
				Type:     "string",
				Required: false,
				Desc:     "需要下载的文件URL（单个URL）",
			},
			"asset_id": {
				Type:     "number",
				Required: false,
				Desc:     "资产台账ID，优先于 file_url",
			},
			"oss_key": {
				Type:     "string",
				Required: false,
				Desc:     "OSS Object Key，优先于 file_url",
			},
			"retry_count": {
				Type:     "int",
				Required: false,
				Desc:     "下载重试次数",
			},
			"allow_failure": {
				Type:     "bool",
				Required: false,
				Desc:     "失败时是否返回状态而不是中断工作流，默认 false",
			},
		},
	}
}

func (t *SignalResourceDownloadTool) OutputSchema() tool.DataSchema {
	return tool.DataSchema{
		Fields: map[string]tool.FieldSchema{
			"local_path": {
				Type: "string",
				Desc: "下载后的本地文件路径",
			},
			"download_status": {
				Type: "string",
				Desc: "success / failed",
			},
			"download_failure_reason": {
				Type: "string",
				Desc: "下载失败原因",
			},
		},
	}
}

func (t *SignalResourceDownloadTool) Execute(ctx context.Context, input map[string]any, emitter tool.ToolEmitter) (*tool.Result, error) {
	// 1. 解析输入
	allowFailure := false
	if v, ok := input["allow_failure"].(bool); ok {
		allowFailure = v
	}
	fileURL, _ := input["file_url"].(string)
	ossKey, _ := input["oss_key"].(string)
	assetID := int64FromAny(input["asset_id"])
	if strings.TrimSpace(fileURL) == "" && strings.TrimSpace(ossKey) == "" && assetID <= 0 {
		err := fmt.Errorf("file_url, oss_key or asset_id is required")
		tool.EmitFail(emitter, err, input)
		if allowFailure {
			return &tool.Result{
				Success: true,
				Data: map[string]any{
					"local_path":              "",
					"download_status":         "failed",
					"download_failure_reason": err.Error(),
				},
			}, nil
		}
		return nil, err
	}
	retryCount, _ := input["retry_count"].(int)
	if retryCount <= 0 {
		retryCount = 3
	}
	tool.EmitStart(emitter, "开始下载文件", map[string]any{
		"file_url": fileURL,
		"oss_key":  ossKey,
		"asset_id": assetID,
	})
	// 2. 构造本地路径
	// 当只有 asset_id（ossKey/fileURL 均为空）时，扩展名在下载前未知，
	// 先用占位扩展名 .tmp，下载后再根据 res.OSSKey 修正。
	extSource := firstNonEmptyDownload(ossKey, fileURL)
	ext := cleanExt(filepath.Ext(extSource))
	if ext == "" {
		ext = ".tmp"
	}
	localPath := fmt.Sprintf("/tmp/download_%d%s", time.Now().UnixNano(), ext)

	if strings.TrimSpace(ossKey) != "" || assetID > 0 {
		if t.ossClient == nil {
			err := fmt.Errorf("oss download requires oss client")
			tool.EmitFail(emitter, err, input)
			if allowFailure {
				return &tool.Result{Success: true, Data: map[string]any{
					"local_path":              "",
					"download_status":         "failed",
					"download_failure_reason": err.Error(),
				}}, nil
			}
			return nil, err
		}
		ownerID := int64(0)
		if meta, ok := tool.ExecutionMetaFromContext(ctx); ok {
			ownerID = meta.UserID
		}
		res, err := t.ossClient.DownloadToFile(localPath, oss.DownloadOptions{
			ObjectKey: strings.TrimSpace(ossKey),
			AssetID:   assetID,
			OwnerID:   ownerID,
		})
		if err != nil {
			tool.EmitFail(emitter, err, map[string]any{"oss_key": ossKey, "asset_id": assetID})
			if allowFailure {
				return &tool.Result{Success: true, Data: map[string]any{
					"local_path":              "",
					"download_status":         "failed",
					"download_failure_reason": err.Error(),
				}}, nil
			}
			return nil, err
		}
		// Fix extension: when only asset_id was given the local path used ".tmp".
		// Now that we have res.OSSKey, rename the file to the correct extension.
		if res.OSSKey != "" {
			if actualExt := cleanExt(filepath.Ext(res.OSSKey)); actualExt != "" {
				if currentExt := filepath.Ext(res.LocalPath); !strings.EqualFold(actualExt, currentExt) {
					correctedPath := strings.TrimSuffix(res.LocalPath, currentExt) + actualExt
					if renameErr := os.Rename(res.LocalPath, correctedPath); renameErr == nil {
						res.LocalPath = correctedPath
					}
				}
			}
		}
		tool.EmitComplete(emitter, "资源下载成功", map[string]interface{}{
			"local_path": res.LocalPath,
			"oss_key":    res.OSSKey,
			"asset_id":   res.AssetID,
		})
		return &tool.Result{
			Data: map[string]any{
				"local_path":      res.LocalPath,
				"oss_key":         res.OSSKey,
				"asset_id":        res.AssetID,
				"file_size":       res.FileSize,
				"download_status": "success",
			},
		}, nil
	}

	// 3. 带重试的下载逻辑
	var err error
	for i := 0; i < retryCount; i++ {
		tool.EmitProgress(emitter, float64(i+1)/float64(retryCount), map[string]interface{}{
			"retry_times": i + 1,
			"url":         fileURL,
		})

		err = downloadFileWithClient(fileURL, localPath)
		if err == nil {
			tool.EmitProgress(emitter, 1, nil)
			// 下载成功
			tool.EmitComplete(emitter, "资源下载成功", map[string]interface{}{
				"local_path": localPath,
			})
			return &tool.Result{
				Data: map[string]any{
					"local_path":      localPath,
					"download_status": "success",
				},
			}, nil
		}

		// 下载失败，重试前处理（如重新获取URL）
		if i < retryCount-1 {
			tool.EmitProgress(emitter, float64(i+1)/float64(retryCount), map[string]interface{}{
				"error":    err.Error(),
				"retrying": true,
			})
			time.Sleep(2 * time.Second) // 重试间隔
		}
	}

	// 所有重试失败
	tool.EmitFail(emitter, err, map[string]interface{}{
		"url":         fileURL,
		"retry_count": retryCount,
	})
	if allowFailure {
		return &tool.Result{
			Success: true,
			Data: map[string]any{
				"local_path":              "",
				"download_status":         "failed",
				"download_failure_reason": err.Error(),
			},
		}, nil
	}
	return nil, fmt.Errorf("下载失败（重试%d次）：%w", retryCount, err)
}

// 带超时的文件下载函数
func downloadFileWithClient(url, localPath string) error {
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("下载失败，状态码：%d", resp.StatusCode)
	}

	file, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.ReadFrom(resp.Body)
	return err
}

func (t *SignalResourceDownloadTool) Mode() tool.ExecutionMode {
	return tool.SyncExecution
}

func firstNonEmptyDownload(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// cleanExt strips query params from a file extension (e.g. ".jpg?v=1" → ".jpg").
func cleanExt(ext string) string {
	if idx := strings.IndexByte(ext, '?'); idx >= 0 {
		ext = ext[:idx]
	}
	return ext
}
