// Package taskapi hosts the business/HTTP-facing task read-model queries
// (pagination, list, detail) that return presentation dto types.
//
// These are intentionally kept OUT of the core repository/query package so
// that the engine core (engine/runtime/worker → repository/query) carries no
// dependency on flux-workflow/dto. Only the HTTP layer (handler/server)
// imports this package.
package taskapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tuxi/flux-workflow/domain"
	"github.com/tuxi/flux-workflow/domain/entity"
	"github.com/tuxi/flux-workflow/dto"
	"github.com/tuxi/flux-workflow/repository"

	"gorm.io/gorm"
)

// TaskQueryRepository extends the core repository.TaskRepository with
// business-facing read-model queries returning presentation dto types.
type TaskQueryRepository interface {
	repository.TaskRepository

	// ListByUser 分页列出某用户的根任务。
	ListByUser(ctx context.Context, userID int64, params dto.PageRequest) ([]*domain.Task, int64, error)

	// ListByUserV2 轻量列表查询。
	ListByUserV2(ctx context.Context, userID int64, req dto.TaskListReq) ([]*dto.Task, int64, error)

	// GetTaskDetail 取任务详情（含物化的 final 结果）。
	GetTaskDetail(ctx context.Context, taskID int64) (*dto.TaskDetail, error)
}

// Repository wraps a core repository.TaskRepository with the dto-returning
// read-model queries, sharing the same *gorm.DB.
type Repository struct {
	repository.TaskRepository
	db *gorm.DB
}

var _ TaskQueryRepository = (*Repository)(nil)

// New builds a TaskQueryRepository over the given DB and core task repository.
func New(db *gorm.DB, core repository.TaskRepository) *Repository {
	return &Repository{TaskRepository: core, db: db}
}

func (r *Repository) ListByUser(
	ctx context.Context,
	userID int64,
	params dto.PageRequest,
) ([]*domain.Task, int64, error) {
	var models []entity.TaskModel
	var total int64

	// 1. 构建基础查询（只查根任务 ParentID IS NULL）
	query := r.db.WithContext(ctx).
		Model(&entity.TaskModel{}).
		Where("user_id = ?", userID).
		Where("parent_id IS NULL")

	// 2. 获取总数（分页前先计算符合条件的记录总数）
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	// 3. 处理排序逻辑
	sortColumn := "id" // 默认排序字段
	if params.Sort != "" {
		sortColumn = params.Sort
	}
	sortOrder := "desc" // 默认倒序
	if params.Order == "asc" {
		sortOrder = "asc"
	}
	orderStr := fmt.Sprintf("%s %s", sortColumn, sortOrder)

	// 4. 执行分页查询
	err := query.Order(orderStr).
		Limit(params.GetLimit()).
		Offset(params.Offset()).
		Find(&models).Error
	if err != nil {
		return nil, 0, err
	}

	// 5. 转换为 Domain 模型
	var result []*domain.Task
	for _, m := range models {
		result = append(result, &domain.Task{
			ID:                   m.ID,
			UserID:               m.UserID,
			Type:                 m.Type,
			Status:               domain.TaskStatus(m.Status),
			InputJSON:            m.InputJSON,
			OutputJSON:           m.OutputJSON,
			WorkflowVersionID:    m.WorkflowVersionID,
			WorkflowDefinitionID: m.WorkflowDefinitionID,
			StartedAt:            m.StartedAt,
			WorkerID:             m.WorkerID,
			RootID:               m.RootID,
			SubKey:               m.SubKey,
			ParentNode:           m.ParentNode,
			MapIndex:             m.MapIndex,
			Progress:             m.Progress,

			BaseRunID:  m.BaseRunID,
			ForkedFrom: m.ForkedFrom,
			RunDepth:   m.RunDepth,
			EditAction: m.EditAction,
			EditLabel:  m.EditLabel,
			ResumeFrom: m.ResumeFrom,
			PatchJSON:  m.PatchJSON,

			RouteKey:          m.RouteKey,
			ModeKey:           m.ModeKey,
			EntryType:         m.EntryType,
			EntryTitle:        m.EntryTitle,
			EntrySubtitle:     m.EntrySubtitle,
			ToolDefinitionID:  m.ToolDefinitionID,
			ToolModeID:        m.ToolModeID,
			ToolModeVersionID: m.ToolModeVersionID,
			TemplateID:        m.TemplateID,
			TemplateVersionID: m.TemplateVersionID,

			CreatedAt: m.CreatedAt,
			UpdatedAt: m.UpdatedAt,
		})
	}

	return result, total, nil
}

func (r *Repository) ListByUserV2(
	ctx context.Context,
	userID int64,
	req dto.TaskListReq,
) ([]*dto.Task, int64, error) {
	page := req.Page
	if page <= 0 {
		page = 1
	}

	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}

	db := r.db.WithContext(ctx).
		Model(&entity.TaskModel{}).
		Where("user_id = ?", userID).
		Where("parent_id IS NULL")

	if req.OnlySuccess != nil && *req.OnlySuccess {
		db = db.Where("status = ?", domain.TaskSuccess)
	} else if req.Status != "" {
		db = db.Where("status = ?", req.Status)
	}

	if req.EntryType != "" {
		db = db.Where("entry_type = ?", req.EntryType)
	}

	if req.RouteKey != "" {
		db = db.Where("route_key = ?", req.RouteKey)
	}
	if keys := splitFilterKeys(req.ExcludeRouteKeys); len(keys) > 0 {
		db = db.Where("route_key NOT IN ?", keys)
	}

	if req.ModeKey != "" {
		db = db.Where("mode_key = ?", req.ModeKey)
	}
	if keys := splitFilterKeys(req.ExcludeModeKeys); len(keys) > 0 {
		db = db.Where("mode_key NOT IN ?", keys)
	}

	if keyword := strings.TrimSpace(req.Keyword); keyword != "" {
		like := "%" + strings.ToLower(keyword) + "%"
		db = db.Where(
			"(LOWER(entry_title) LIKE ? OR LOWER(entry_subtitle) LIKE ? OR LOWER(route_key) LIKE ? OR LOWER(mode_key) LIKE ?)",
			like, like, like, like,
		)
	}

	var models []entity.TaskModel
	query := db.Select(
		"id",
		"user_id",
		"status",
		"type",
		"entry_type",
		"entry_title",
		"entry_subtitle",
		"route_key",
		"mode_key",
		"output_json",
		"input_json",
		"error_message",
		"progress",
		"created_at",
		"updated_at",
	).Order("id DESC")

	var total int64
	if req.ResultType == "" {
		if err := db.Count(&total).Error; err != nil {
			return nil, 0, err
		}
		query = query.Offset((page - 1) * pageSize).Limit(pageSize)
	}

	err := query.Find(&models).Error
	if err != nil {
		return nil, 0, err
	}

	result := make([]*dto.Task, 0, len(models))
	for _, m := range models {
		final, err := domain.ParseFinal(m.OutputJSON)
		var input map[string]interface{}
		_ = json.Unmarshal(m.InputJSON, &input) // 反序列化输入参数，方便前端展示
		if err != nil && req.OnlySuccess != nil && *req.OnlySuccess {
			continue // 要求只获取成功的任务，所以final 必须存在
		}
		if req.ResultType != "" {
			if final == nil || final.ResultType != req.ResultType {
				continue
			}
		}
		result = append(result, &dto.Task{
			ID:            m.ID,
			UserID:        userID,
			Status:        m.Status,
			Type:          m.Type,
			EntryType:     &m.EntryType,
			EntryTitle:    m.EntryTitle,
			EntrySubtitle: m.EntrySubtitle,
			RouteKey:      m.RouteKey,
			ModeKey:       m.ModeKey,
			Result:        final,
			Input:         input,
			ErrorMessage:  m.ErrorMessage,
			Progress:      m.Progress,
			CreatedAt:     m.CreatedAt.Unix(),
			UpdatedAt:     m.UpdatedAt.Unix(),
		})
	}

	if req.ResultType != "" {
		total = int64(len(result))
		start := (page - 1) * pageSize
		if start >= len(result) {
			return []*dto.Task{}, total, nil
		}
		end := start + pageSize
		if end > len(result) {
			end = len(result)
		}
		result = result[start:end]
	}

	return result, total, nil
}

func (r *Repository) GetTaskDetail(ctx context.Context, taskID int64) (*dto.TaskDetail, error) {
	db := r.db.WithContext(ctx).
		Model(&entity.TaskModel{}).
		Where("id = ?", taskID).
		Where("parent_id IS NULL")

	var m entity.TaskModel
	query := db.Select(
		"id",
		"status",
		"user_id",
		"type",
		"entry_type",
		"entry_title",
		"entry_subtitle",
		"route_key",
		"mode_key",
		"output_json",
		"input_json",
		"error_message",
		"retry_count",
		"progress",
		"created_at",
		"updated_at",
	)

	err := query.First(&m).Error
	if err != nil {
		return nil, err
	}

	final, _ := domain.ParseFinal(m.OutputJSON)
	var input map[string]interface{}
	_ = json.Unmarshal(m.InputJSON, &input) // 反序列化输入参数，方便前端展示
	return &dto.TaskDetail{
		ID:            m.ID,
		UserID:        m.UserID,
		Status:        m.Status,
		Type:          m.Type,
		EntryType:     &m.EntryType,
		EntryTitle:    m.EntryTitle,
		EntrySubtitle: m.EntrySubtitle,
		RouteKey:      m.RouteKey,
		ModeKey:       m.ModeKey,
		Result:        final,
		Input:         input,
		ErrorMessage:  m.ErrorMessage,
		RetryCount:    m.RetryCount,
		Progress:      m.Progress,
		CreatedAt:     m.CreatedAt.Unix(),
		UpdatedAt:     m.UpdatedAt.Unix(),
	}, nil
}

func splitFilterKeys(raw string) []string {
	parts := strings.Split(raw, ",")
	keys := make([]string, 0, len(parts))
	for _, part := range parts {
		key := strings.TrimSpace(part)
		if key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}
