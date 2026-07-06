package service

import (
	"context"
	"encoding/json"
	"flux-workflow/domain"
	"flux-workflow/engine"
	"flux-workflow/repository"
	"flux-workflow/workflow/nodes"
	"fmt"
	"strings"

	"github.com/tuxi/flux/definition"

	"github.com/tuxi/flux/tool"
)

type CreativeDetailService interface {
	BuildTaskCreativeDetail(ctx context.Context, taskID int64) (*domain.CreativeDetail, error)
	WithAssetSigner(assetSigner StorageAssetService) CreativeDetailService
}

type creativeDetailURLSigner interface {
	SignURLsInValue(ctx context.Context, userID int64, value any) any
	HydrateAssetRefs(ctx context.Context, userID int64, value any) any
}

type creativeDetailService struct {
	taskRepo            repository.TaskRepository
	workflowVersionRepo repository.WorkflowVersionRepository
	replayEngine        *engine.Engine
	toolRegistry        *tool.Registry
	assetSigner         creativeDetailURLSigner
}

func NewCreativeDetailService(
	taskRepo repository.TaskRepository,
	workflowVersionRepo repository.WorkflowVersionRepository,
	replayEngine *engine.Engine,
	toolRegistry *tool.Registry,
) CreativeDetailService {
	return &creativeDetailService{
		taskRepo:            taskRepo,
		workflowVersionRepo: workflowVersionRepo,
		replayEngine:        replayEngine,
		toolRegistry:        toolRegistry,
	}
}

func (s *creativeDetailService) WithAssetSigner(assetSigner StorageAssetService) CreativeDetailService {
	if s != nil {
		s.assetSigner = assetSigner
	}
	return s
}

func (s *creativeDetailService) BuildTaskCreativeDetail(ctx context.Context, taskID int64) (*domain.CreativeDetail, error) {
	if s == nil {
		return nil, fmt.Errorf("creative detail service is nil")
	}
	if s.replayEngine == nil {
		return nil, fmt.Errorf("creative detail replay engine is nil")
	}
	if s.toolRegistry == nil {
		return nil, fmt.Errorf("creative detail tool registry is nil")
	}

	task, err := s.taskRepo.GetByID(ctx, taskID)
	if err != nil {
		return nil, err
	}
	if task == nil {
		return nil, fmt.Errorf("task not found: %d", taskID)
	}

	def, err := s.loadWorkflowDefinition(ctx, task.WorkflowVersionID)
	if err != nil {
		return nil, err
	}

	builderDef, err := resolveCreativeDetailBuilder(def)
	if err != nil {
		// 兼容旧数据：如果旧 task output 里已经有 creative_detail，允许读取兜底。
		if fallback := parseLegacyTaskCreativeDetail(task.OutputJSON); fallback != nil {
			return s.signCreativeDetailURLs(ctx, task.UserID, fallback), nil
		}
		return nil, err
	}

	t, ok := s.toolRegistry.Get(builderDef.Tool)
	if !ok {
		return nil, fmt.Errorf("creative detail tool not found: %s", builderDef.Tool)
	}

	trace, err := s.replayEngine.Replay(ctx, taskID)
	if err != nil {
		return nil, err
	}

	replayCtx := buildCreativeDetailReplayContext(ctx, task, def, trace)
	input, err := resolveCreativeDetailInput(replayCtx, builderDef)
	if err != nil {
		return nil, err
	}

	execCtx := tool.ContextWithExecutionMeta(ctx, tool.ExecutionMeta{
		UserID:   task.UserID,
		TaskID:   task.ID,
		RootID:   task.RootID,
		NodeName: "creative_detail",
	})
	result, err := t.Execute(execCtx, input, noopToolEmitter{})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("creative detail tool returned nil result")
	}

	detail, err := domain.ParseCreativeDetail(result.Data["creative_detail"])
	if err != nil {
		return nil, err
	}
	if detail == nil {
		return nil, fmt.Errorf("creative detail tool returned empty creative_detail")
	}
	return s.signCreativeDetailURLs(ctx, task.UserID, detail), nil
}

func (s *creativeDetailService) signCreativeDetailURLs(ctx context.Context, userID int64, detail *domain.CreativeDetail) *domain.CreativeDetail {
	if s == nil || s.assetSigner == nil || detail == nil {
		return detail
	}
	raw, err := json.Marshal(detail)
	if err != nil {
		return detail
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return detail
	}
	// Sign legacy OSS URLs first, then inject URLs for asset_id-first fields.
	value = s.assetSigner.SignURLsInValue(ctx, userID, value)
	value = s.assetSigner.HydrateAssetRefs(ctx, userID, value)
	signedRaw, err := json.Marshal(value)
	if err != nil {
		return detail
	}
	var out domain.CreativeDetail
	if err := json.Unmarshal(signedRaw, &out); err != nil {
		return detail
	}
	return &out
}

func (s *creativeDetailService) loadWorkflowDefinition(ctx context.Context, workflowVersionID int64) (*definition.WorkflowDefinition, error) {
	dbVersion, err := s.workflowVersionRepo.Get(ctx, workflowVersionID)
	if err != nil {
		return nil, err
	}
	if dbVersion == nil {
		return nil, fmt.Errorf("workflow version not found: %d", workflowVersionID)
	}
	var def definition.WorkflowDefinition
	if err := json.Unmarshal(dbVersion.DefinitionJSON, &def); err != nil {
		return nil, err
	}
	return &def, nil
}

// outputSliceCreativeDetail 是 creative_detail 视图在 definition.OutputSlices 中的键。
const outputSliceCreativeDetail = "creative_detail"

func resolveCreativeDetailBuilder(def *definition.WorkflowDefinition) (*definition.OutputSliceDefinition, error) {
	if def == nil {
		return nil, fmt.Errorf("workflow definition is nil")
	}
	if slice := def.OutputSlices[outputSliceCreativeDetail]; slice != nil && strings.TrimSpace(slice.Tool) != "" {
		return slice, nil
	}

	for _, nodeDef := range def.Nodes {
		if nodeDef.Name != "build_creative_detail" {
			continue
		}
		toolName, _ := nodeDef.Config["tool"].(string)
		if strings.TrimSpace(toolName) == "" {
			return nil, fmt.Errorf("legacy build_creative_detail node missing config.tool")
		}
		return &definition.OutputSliceDefinition{
			Tool:         toolName,
			Config:       nodeDef.Config,
			InputMapping: nodeDef.InputMapping,
			Version:      nodeDef.Version,
		}, nil
	}

	return nil, fmt.Errorf("creative detail builder not configured")
}

func buildCreativeDetailReplayContext(
	ctx context.Context,
	task *domain.Task,
	def *definition.WorkflowDefinition,
	trace *engine.ReplayTrace,
) *nodes.Context {
	input := map[string]any{}
	if trace != nil && trace.Input != nil {
		input = trace.Input
	} else if task != nil && len(task.InputJSON) > 0 {
		_ = json.Unmarshal(task.InputJSON, &input)
	}

	replayCtx := &nodes.Context{
		Workflow: def,
		Ctx:      ctx,
		Task:     task,
		Input:    input,
		Output: map[string]any{
			"input": input,
			"nodes": map[string]any{},
		},
		Runtime: make(map[string]*domain.NodeRuntime),
	}

	nodesMap := replayCtx.Output["nodes"].(map[string]any)
	if trace != nil {
		for _, frame := range trace.Nodes {
			nodesMap[frame.Name] = map[string]any{
				"status": string(frame.State),
				"output": frame.Output,
			}
			replayCtx.Runtime[frame.Name] = &domain.NodeRuntime{
				TaskID:   task.ID,
				Name:     frame.Name,
				State:    frame.State,
				Index:    frame.Index,
				BizIndex: frame.BizIndex,
				Output:   frame.Output,
			}
		}
	}
	return replayCtx
}

func resolveCreativeDetailInput(
	ctx *nodes.Context,
	builderDef *definition.OutputSliceDefinition,
) (map[string]any, error) {
	if ctx == nil {
		return nil, fmt.Errorf("creative detail context is nil")
	}
	if builderDef == nil {
		return nil, fmt.Errorf("creative detail builder is nil")
	}
	resolved := make(map[string]any)
	for k, v := range builderDef.Config {
		if k == "tool" || k == "persist_output" || k == "retry_count" || k == "retry_interval_ms" {
			continue
		}
		resolved[k] = v
	}
	for targetField, source := range builderDef.InputMapping {
		val, err := ctx.EvalAny(source)
		if err != nil {
			return nil, fmt.Errorf("creative detail inputMapping %s -> %s error: %w", targetField, source, err)
		}
		resolved[targetField] = val
	}
	return resolved, nil
}

// parseLegacyTaskCreativeDetail 从旧版持久化的 task 输出 JSON 中直接读取
// 内嵌的 creative_detail。creative_detail 已不再是核心 TaskOutput 的字段
// （见 domain.TaskOutput 注释），因此这里自行按旧格式解析，不再依赖核心输出结构。
func parseLegacyTaskCreativeDetail(outputJSON []byte) *domain.CreativeDetail {
	if len(outputJSON) == 0 {
		return nil
	}

	// 旧格式：{"final": {"creative_detail": {...}}} 或顶层 {"creative_detail": {...}}
	var envelope struct {
		Final struct {
			CreativeDetail *domain.CreativeDetail `json:"creative_detail"`
		} `json:"final"`
		CreativeDetail *domain.CreativeDetail `json:"creative_detail"`
	}
	if err := json.Unmarshal(outputJSON, &envelope); err != nil {
		return nil
	}
	if envelope.Final.CreativeDetail != nil {
		return envelope.Final.CreativeDetail
	}
	return envelope.CreativeDetail
}

type noopToolEmitter struct{}

func (noopToolEmitter) EmitToolEvent(event tool.ToolEvent) {}
