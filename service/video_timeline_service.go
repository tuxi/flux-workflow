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

type VideoTimelineService interface {
	BuildTaskVideoTimeline(ctx context.Context, taskID int64) (map[string]any, error)
	WithAssetSigner(assetSigner StorageAssetService) VideoTimelineService
}

type videoTimelineService struct {
	taskRepo            repository.TaskRepository
	workflowVersionRepo repository.WorkflowVersionRepository
	replayEngine        *engine.Engine
	toolRegistry        *tool.Registry
	assetSigner         creativeDetailURLSigner
}

func NewVideoTimelineService(
	taskRepo repository.TaskRepository,
	workflowVersionRepo repository.WorkflowVersionRepository,
	replayEngine *engine.Engine,
	toolRegistry *tool.Registry,
) VideoTimelineService {
	return &videoTimelineService{
		taskRepo:            taskRepo,
		workflowVersionRepo: workflowVersionRepo,
		replayEngine:        replayEngine,
		toolRegistry:        toolRegistry,
	}
}

func (s *videoTimelineService) WithAssetSigner(assetSigner StorageAssetService) VideoTimelineService {
	if s != nil {
		s.assetSigner = assetSigner
	}
	return s
}

func (s *videoTimelineService) BuildTaskVideoTimeline(ctx context.Context, taskID int64) (map[string]any, error) {
	if s == nil {
		return nil, fmt.Errorf("video timeline service is nil")
	}
	if s.replayEngine == nil {
		return nil, fmt.Errorf("video timeline replay engine is nil")
	}
	if s.toolRegistry == nil {
		return nil, fmt.Errorf("video timeline tool registry is nil")
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

	builderDef, err := resolveTimelineBuilder(def)
	if err != nil {
		return nil, err
	}

	t, ok := s.toolRegistry.Get(builderDef.Tool)
	if !ok {
		return nil, fmt.Errorf("video timeline tool not found: %s", builderDef.Tool)
	}

	trace, err := s.replayEngine.Replay(ctx, taskID)
	if err != nil {
		return nil, err
	}

	replayCtx := buildVideoTimelineReplayContext(ctx, task, def, trace)
	input, err := resolveTimelineInput(replayCtx, builderDef)
	if err != nil {
		return nil, err
	}

	execCtx := tool.ContextWithExecutionMeta(ctx, tool.ExecutionMeta{
		UserID:   task.UserID,
		TaskID:   task.ID,
		RootID:   task.RootID,
		NodeName: "video_timeline",
	})
	result, err := t.Execute(execCtx, input, noopToolEmitter{})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("video timeline tool returned nil result")
	}

	timeline, ok := result.Data["timeline"]
	if !ok || timeline == nil {
		return nil, fmt.Errorf("video timeline tool returned empty timeline")
	}
	response := map[string]any{
		"timeline": timeline,
	}
	if totalDuration, ok := result.Data["total_duration"]; ok {
		response["total_duration"] = totalDuration
	}
	return s.signTimelineURLs(ctx, task.UserID, response), nil
}

func (s *videoTimelineService) signTimelineURLs(ctx context.Context, userID int64, value map[string]any) map[string]any {
	if s == nil || s.assetSigner == nil || value == nil {
		return value
	}
	// Sign legacy OSS URLs first, then inject URLs for asset_id-first fields.
	out := s.assetSigner.SignURLsInValue(ctx, userID, value)
	out = s.assetSigner.HydrateAssetRefs(ctx, userID, out)
	if m, ok := out.(map[string]any); ok {
		return m
	}
	raw, err := json.Marshal(out)
	if err != nil {
		return value
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return value
	}
	return m
}

func (s *videoTimelineService) loadWorkflowDefinition(ctx context.Context, workflowVersionID int64) (*definition.WorkflowDefinition, error) {
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

func resolveTimelineBuilder(def *definition.WorkflowDefinition) (*definition.TimelineBuilderDefinition, error) {
	if def == nil {
		return nil, fmt.Errorf("workflow definition is nil")
	}
	if def.TimelineBuilder != nil && strings.TrimSpace(def.TimelineBuilder.Tool) != "" {
		return def.TimelineBuilder, nil
	}

	for _, nodeDef := range def.Nodes {
		if nodeDef.Name != "build_video_timeline" {
			continue
		}
		toolName, _ := nodeDef.Config["tool"].(string)
		if strings.TrimSpace(toolName) == "" {
			return nil, fmt.Errorf("legacy build_video_timeline node missing config.tool")
		}
		return &definition.TimelineBuilderDefinition{
			Tool:         toolName,
			Config:       nodeDef.Config,
			InputMapping: nodeDef.InputMapping,
			Version:      nodeDef.Version,
		}, nil
	}

	if def.Name == "goods_video_pro_v3" {
		// 兼容旧数据
		def.TimelineBuilder = &definition.TimelineBuilderDefinition{
			Tool: "build_goods_video_pro_timeline",
			Config: map[string]any{
				"tool": "build_goods_video_pro_timeline",
			},
			InputMapping: map[string]string{
				"shots":         "loop_generate_shots_v3.results",
				"video_script":  "validate_shot_plan_quality.video_script",
				"subtitle_plan": "align_voiceover_timeline.subtitle_plan ?? build_subtitle_timeline.subtitle_plan",
				//"voice_audio_url":           "upload_voice_audio.url",
				"bgm_url":                   "input.bgm_url ?? ''",
				"aspect_ratio":              "input.aspect_ratio",
				"fps":                       "input.fps ?? 30",
				"platform_style":            "nodes.platform_style_resolver.output",
				"product_name":              "resolve_product_metadata.product_name ?? input.product_name",
				"selling_points":            "input.selling_points",
				"mode":                      "input.mode",
				"resolution":                "input.resolution",
				"transition_enabled":        "input.transition_enabled ?? true",
				"transition_name":           "input.transition_name ?? 'fade'",
				"transition_duration":       "input.transition_duration ?? 0.25",
				"audio_transition_duration": "input.audio_transition_duration ?? 0.08",
			},
		}
		return def.TimelineBuilder, nil
	}

	return nil, fmt.Errorf("video timeline builder not configured")
}

func buildVideoTimelineReplayContext(
	ctx context.Context,
	task *domain.Task,
	def *definition.WorkflowDefinition,
	trace *engine.ReplayTrace,
) *nodes.Context {
	return buildCreativeDetailReplayContext(ctx, task, def, trace)
}

func resolveTimelineInput(
	ctx *nodes.Context,
	builderDef *definition.TimelineBuilderDefinition,
) (map[string]any, error) {
	if ctx == nil {
		return nil, fmt.Errorf("video timeline context is nil")
	}
	if builderDef == nil {
		return nil, fmt.Errorf("video timeline builder is nil")
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
			return nil, fmt.Errorf("video timeline inputMapping %s -> %s error: %w", targetField, source, err)
		}
		resolved[targetField] = val
	}
	return resolved, nil
}
