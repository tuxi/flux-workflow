package query

import (
	"context"
	"fmt"
	"time"

	"github.com/tuxi/flux-workflow/domain"
	"github.com/tuxi/flux-workflow/domain/entity"
	"github.com/tuxi/flux-workflow/repository"

	"github.com/tuxi/flux-workflow/utils"

	"gorm.io/gorm"
)

type taskRepository struct {
	db    *gorm.DB
	queue repository.TaskQueue
}

// NewTaskRepository 返回核心 repository.TaskRepository 实现。业务侧的分页/
// 详情查询（返回 dto）见 repository/query/taskapi，与核心存储解耦。
func NewTaskRepository(db *gorm.DB, queue repository.TaskQueue) repository.TaskRepository {
	return &taskRepository{db: db, queue: queue}
}

func (r *taskRepository) Create(ctx context.Context, task *domain.Task) error {

	model := entity.TaskModel{
		ID:                   task.ID,
		RootID:               task.RootID,
		Type:                 task.Type,
		UserID:               task.UserID,
		Status:               string(task.Status),
		RetryCount:           task.RetryCount,
		InputJSON:            task.InputJSON,
		ParentID:             task.ParentID,
		WorkflowVersionID:    task.WorkflowVersionID,
		WorkflowDefinitionID: task.WorkflowDefinitionID,
		SubKey:               task.SubKey,
		ParentNode:           task.ParentNode,
		WorkerID:             task.WorkerID,
		StartedAt:            task.StartedAt,
		MapIndex:             task.MapIndex,
		Progress:             task.Progress,

		BaseRunID:  task.BaseRunID,
		ForkedFrom: task.ForkedFrom,
		RunDepth:   task.RunDepth,
		EditAction: task.EditAction,
		EditLabel:  task.EditLabel,
		ResumeFrom: task.ResumeFrom,
		PatchJSON:  task.PatchJSON,

		RouteKey:           task.RouteKey,
		ModeKey:            task.ModeKey,
		EntryType:          task.EntryType,
		EntryTitle:         task.EntryTitle,
		EntrySubtitle:      task.EntrySubtitle,
		ToolDefinitionID:   task.ToolDefinitionID,
		ToolModeID:         task.ToolModeID,
		ToolModeVersionID:  task.ToolModeVersionID,
		TemplateID:         task.TemplateID,
		TemplateVersionID:  task.TemplateVersionID,
		EstimatedCostTotal: task.EstimatedCostTotal,
		ActualCostTotal:    task.ActualCostTotal,
		CostStatus:         task.CostStatus,
	}

	err := r.db.WithContext(ctx).Create(&model).Error
	if err != nil {
		return err
	}

	task.ID = model.ID
	return nil
}

func (r *taskRepository) GetByID(ctx context.Context, id int64) (*domain.Task, error) {

	var model entity.TaskModel
	err := r.db.WithContext(ctx).First(&model, id).Error
	if err != nil {
		return nil, err
	}

	return &domain.Task{
		ID:                   model.ID,
		UserID:               model.UserID,
		Type:                 model.Type,
		Status:               domain.TaskStatus(model.Status),
		RetryCount:           model.RetryCount,
		InputJSON:            model.InputJSON,
		OutputJSON:           model.OutputJSON,
		ParentID:             model.ParentID,
		WorkflowVersionID:    model.WorkflowVersionID,
		WorkflowDefinitionID: model.WorkflowDefinitionID,
		RootID:               model.RootID,
		SubKey:               model.SubKey,
		ParentNode:           model.ParentNode,
		MapIndex:             model.MapIndex,
		Progress:             model.Progress,

		BaseRunID:  model.BaseRunID,
		ForkedFrom: model.ForkedFrom,
		RunDepth:   model.RunDepth,
		EditAction: model.EditAction,
		EditLabel:  model.EditLabel,
		ResumeFrom: model.ResumeFrom,
		PatchJSON:  model.PatchJSON,

		RouteKey:           model.RouteKey,
		ModeKey:            model.ModeKey,
		EntryType:          model.EntryType,
		EntryTitle:         model.EntryTitle,
		EntrySubtitle:      model.EntrySubtitle,
		ToolDefinitionID:   model.ToolDefinitionID,
		ToolModeID:         model.ToolModeID,
		ToolModeVersionID:  model.ToolModeVersionID,
		TemplateID:         model.TemplateID,
		TemplateVersionID:  model.TemplateVersionID,
		EstimatedCostTotal: model.EstimatedCostTotal,
		ActualCostTotal:    model.ActualCostTotal,
		CostStatus:         model.CostStatus,

		CreatedAt: model.CreatedAt,
		UpdatedAt: model.UpdatedAt,
	}, nil
}

func (r *taskRepository) Update(ctx context.Context, task *domain.Task) error {

	err := r.db.WithContext(ctx).
		Model(&entity.TaskModel{}).
		Where("id = ?", task.ID).
		Updates(map[string]interface{}{
			"status":                 task.Status,
			"retry_count":            task.RetryCount,
			"updated_at":             time.Now(),
			"input_json":             task.InputJSON,
			"output_json":            task.OutputJSON,
			"parent_id":              task.ParentID,
			"error_message":          task.ErrorMessage,
			"workflow_version_id":    task.WorkflowVersionID,
			"workflow_definition_id": task.WorkflowDefinitionID,
			"root_id":                task.RootID,
			"sub_key":                task.SubKey,
			"parent_node":            task.ParentNode,
			"map_index":              task.MapIndex,
			"progress":               task.Progress,

			"base_run_id": task.BaseRunID,
			"forked_from": task.ForkedFrom,
			"run_depth":   task.RunDepth,
			"edit_action": task.EditAction,
			"edit_label":  task.EditLabel,
			"resume_from": task.ResumeFrom,
			"patch_json":  task.PatchJSON,

			"route_key":            task.RouteKey,
			"mode_key":             task.ModeKey,
			"entry_type":           task.EntryType,
			"entry_title":          task.EntryTitle,
			"entry_subtitle":       task.EntrySubtitle,
			"tool_definition_id":   task.ToolDefinitionID,
			"tool_mode_id":         task.ToolModeID,
			"tool_mode_version_id": task.ToolModeVersionID,
			"template_id":          task.TemplateID,
			"template_version_id":  task.TemplateVersionID,
			"estimated_cost_total": task.EstimatedCostTotal,
			"actual_cost_total":    task.ActualCostTotal,
			"cost_status":          task.CostStatus,
		}).Error
	return err
}

func (r *taskRepository) ListByParent(ctx context.Context, parentID int64) ([]*domain.Task, error) {

	var models []entity.TaskModel
	err := r.db.WithContext(ctx).
		Where("parent_id = ?", parentID).
		Find(&models).Error

	if err != nil {
		return nil, err
	}

	var result []*domain.Task
	for _, m := range models {
		result = append(result, &domain.Task{
			ID:                   m.ID,
			UserID:               m.UserID,
			Type:                 m.Type,
			Status:               domain.TaskStatus(m.Status),
			RetryCount:           m.RetryCount,
			InputJSON:            m.InputJSON,
			OutputJSON:           m.OutputJSON,
			ErrorMessage:         utils.ValueOrEmpty(m.ErrorMessage),
			ParentID:             m.ParentID,
			WorkflowVersionID:    m.WorkflowVersionID,
			WorkflowDefinitionID: m.WorkflowDefinitionID,
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

			RouteKey:           m.RouteKey,
			ModeKey:            m.ModeKey,
			EntryType:          m.EntryType,
			EntryTitle:         m.EntryTitle,
			EntrySubtitle:      m.EntrySubtitle,
			ToolDefinitionID:   m.ToolDefinitionID,
			ToolModeID:         m.ToolModeID,
			ToolModeVersionID:  m.ToolModeVersionID,
			TemplateID:         m.TemplateID,
			TemplateVersionID:  m.TemplateVersionID,
			EstimatedCostTotal: m.EstimatedCostTotal,
			ActualCostTotal:    m.ActualCostTotal,
			CostStatus:         m.CostStatus,

			CreatedAt: m.CreatedAt,
			UpdatedAt: m.UpdatedAt,
		})
	}

	return result, nil
}

func (r *taskRepository) ListByRoot(ctx context.Context, rootID int64) ([]*domain.Task, error) {

	var models []entity.TaskModel
	err := r.db.WithContext(ctx).
		Where("root_id = ?", rootID).
		Find(&models).Error

	if err != nil {
		return nil, err
	}

	var result []*domain.Task
	for _, m := range models {
		result = append(result, &domain.Task{
			ID:                   m.ID,
			UserID:               m.UserID,
			RootID:               m.RootID,
			Type:                 m.Type,
			Status:               domain.TaskStatus(m.Status),
			RetryCount:           m.RetryCount,
			InputJSON:            m.InputJSON,
			OutputJSON:           m.OutputJSON,
			ErrorMessage:         utils.ValueOrEmpty(m.ErrorMessage),
			ParentID:             m.ParentID,
			WorkflowVersionID:    m.WorkflowVersionID,
			WorkflowDefinitionID: m.WorkflowDefinitionID,
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

			RouteKey:           m.RouteKey,
			ModeKey:            m.ModeKey,
			EntryType:          m.EntryType,
			EntryTitle:         m.EntryTitle,
			EntrySubtitle:      m.EntrySubtitle,
			ToolDefinitionID:   m.ToolDefinitionID,
			ToolModeID:         m.ToolModeID,
			ToolModeVersionID:  m.ToolModeVersionID,
			TemplateID:         m.TemplateID,
			TemplateVersionID:  m.TemplateVersionID,
			EstimatedCostTotal: m.EstimatedCostTotal,
			ActualCostTotal:    m.ActualCostTotal,
			CostStatus:         m.CostStatus,

			CreatedAt: m.CreatedAt,
			UpdatedAt: m.UpdatedAt,
		})
	}

	return result, nil
}

func (r *taskRepository) FindRunningRootTasks(ctx context.Context, before time.Time) ([]*domain.Task, error) {
	var models []entity.TaskModel
	err := r.db.WithContext(ctx).
		Where("updated_at < ?", before).
		Where("status = ?", domain.TaskRunning).
		Where("root_id IS NULL").
		Find(&models).
		Error
	if err != nil {
		return nil, err
	}
	var result []*domain.Task
	for _, m := range models {
		result = append(result, &domain.Task{
			ID:                   m.ID,
			UserID:               m.UserID,
			Type:                 m.Type,
			Status:               domain.TaskStatus(m.Status),
			RetryCount:           m.RetryCount,
			InputJSON:            m.InputJSON,
			OutputJSON:           m.OutputJSON,
			ErrorMessage:         utils.ValueOrEmpty(m.ErrorMessage),
			WorkflowVersionID:    m.WorkflowVersionID,
			WorkflowDefinitionID: m.WorkflowDefinitionID,
			ParentID:             m.ParentID,
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
	return result, nil
}

func (r *taskRepository) FindByWorkflowID(ctx context.Context, workflowID int64) ([]*domain.Task, error) {
	var models []entity.TaskModel
	err := r.db.WithContext(ctx).
		Where("workflow_definition_id = ?", workflowID).
		Where("status = ?", domain.TaskRunning).
		Find(&models).
		Error
	if err != nil {
		return nil, err
	}
	var result []*domain.Task
	for _, m := range models {
		result = append(result, &domain.Task{
			ID:                   m.ID,
			UserID:               m.UserID,
			Type:                 m.Type,
			Status:               domain.TaskStatus(m.Status),
			RetryCount:           m.RetryCount,
			InputJSON:            m.InputJSON,
			OutputJSON:           m.OutputJSON,
			ErrorMessage:         utils.ValueOrEmpty(m.ErrorMessage),
			WorkflowVersionID:    m.WorkflowVersionID,
			WorkflowDefinitionID: m.WorkflowDefinitionID,
			ParentID:             m.ParentID,
			WorkerID:             m.WorkerID,
			StartedAt:            m.StartedAt,
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
	return result, nil
}

// TryClaimTask 以原子化方式尝试抢占一个待处理任务，确保并发环境下只有一个工作节点能成功接管该任务，从而避免任务重复执行‌。
// 这是一种典型的“任务锁”或“工作抢占”模式，广泛应用于分布式任务队列、微服务调度系统中，保障任务处理的唯一性与一致性。
func (r *taskRepository) TryClaimTask(
	ctx context.Context,
	taskID int64,
	workerID string,
) (bool, error) {

	now := time.Now()
	// 运行超时任务允许重新抢占
	leaseExpire := now.Add(-10 * time.Minute)

	result := r.db.WithContext(ctx).
		Model(&entity.TaskModel{}).
		Where(
			// 防止重复 Claim Running 任务到一种情况:
			//worker crash
			//task status = running
			//queue recovery
			//重新投递
			// 此时task status = running
			// Worker 就 永远抢不到任务。
			// 解决方案：允许 running 但超时任务重新 claim。
			"id = ? AND (status = ? OR (status = ? AND started_at < ?))",
			taskID,
			domain.TaskPending,
			domain.TaskRunning,
			leaseExpire,
		).
		Updates(map[string]interface{}{
			"status":     domain.TaskRunning,
			"worker_id":  workerID,
			"started_at": now,
			"updated_at": now,
		})

	if result.Error != nil {
		return false, result.Error
	}
	// rows == 1 说明抢占成功
	rows := result.RowsAffected
	return rows == 1, nil
}

func (r *taskRepository) FindBySubKey(
	ctx context.Context,
	subKey string,
) (*domain.Task, error) {

	var model entity.TaskModel
	err := r.db.WithContext(ctx).
		Where("sub_key = ?", subKey).
		First(&model).Error

	if err != nil {
		return nil, err
	}

	return &domain.Task{
		ID:                   model.ID,
		UserID:               model.UserID,
		Status:               domain.TaskStatus(model.Status),
		InputJSON:            model.InputJSON,
		OutputJSON:           model.OutputJSON,
		ParentID:             model.ParentID,
		RootID:               model.RootID,
		WorkflowVersionID:    model.WorkflowVersionID,
		WorkflowDefinitionID: model.WorkflowDefinitionID,
		SubKey:               model.SubKey,
		WorkerID:             model.WorkerID,
		StartedAt:            model.StartedAt,
		ParentNode:           model.ParentNode,
		MapIndex:             model.MapIndex,
		Progress:             model.Progress,

		BaseRunID:  model.BaseRunID,
		ForkedFrom: model.ForkedFrom,
		RunDepth:   model.RunDepth,
		EditAction: model.EditAction,
		EditLabel:  model.EditLabel,
		ResumeFrom: model.ResumeFrom,
		PatchJSON:  model.PatchJSON,

		RouteKey:          model.RouteKey,
		ModeKey:           model.ModeKey,
		EntryType:         model.EntryType,
		EntryTitle:        model.EntryTitle,
		EntrySubtitle:     model.EntrySubtitle,
		ToolDefinitionID:  model.ToolDefinitionID,
		ToolModeID:        model.ToolModeID,
		ToolModeVersionID: model.ToolModeVersionID,
		TemplateID:        model.TemplateID,
		TemplateVersionID: model.TemplateVersionID,

		CreatedAt: model.CreatedAt,
		UpdatedAt: model.UpdatedAt,
	}, nil
}

func (r *taskRepository) ListByParentNode(
	ctx context.Context,
	parentID int64,
	nodeName string,
) ([]*domain.Task, error) {

	var tasks []*domain.Task

	err := r.db.WithContext(ctx).
		Where("parent_id = ? AND parent_node = ?", parentID, nodeName).
		Order("map_index ASC").
		Find(&tasks).Error

	return tasks, err
}

// Enqueue 任务ID 入队列，只有任务ID入队后才会运行任务
func (r *taskRepository) Enqueue(ctx context.Context, taskID int64) error {
	return r.queue.Push(ctx, taskID)
}

func (r *taskRepository) CreateFork(
	ctx context.Context,
	source *domain.Task,
	newTaskId int64,
	newInput []byte,
	editAction, editLabel string,
) (*domain.Task, error) {

	if source == nil {
		return nil, fmt.Errorf("source task is nil")
	}

	baseRunID := source.BaseRunID
	if baseRunID == 0 {
		baseRunID = source.ID
	}

	now := time.Now()

	task := &domain.Task{
		//ID:                   utils.GenSnowflakeID(),
		ID:                   newTaskId,
		UserID:               source.UserID,
		Type:                 source.Type,
		Status:               domain.TaskPending,
		InputJSON:            newInput,
		WorkflowVersionID:    source.WorkflowVersionID,
		WorkflowDefinitionID: source.WorkflowDefinitionID,
		RootID:               0, // 创建后设成自己
		Progress:             0,
		StartedAt:            now,
		BaseRunID:            baseRunID,
		ForkedFrom:           &source.ID,
		RunDepth:             source.RunDepth + 1,
		EditAction:           source.EditAction,
		EditLabel:            source.EditLabel,
		ResumeFrom:           source.ResumeFrom,
		PatchJSON:            source.PatchJSON,

		RouteKey:          source.RouteKey,
		ModeKey:           source.ModeKey,
		EntryType:         source.EntryType,
		EntryTitle:        source.EntryTitle,
		EntrySubtitle:     source.EntrySubtitle,
		ToolDefinitionID:  source.ToolDefinitionID,
		ToolModeID:        source.ToolModeID,
		ToolModeVersionID: source.ToolModeVersionID,
		TemplateID:        source.TemplateID,
		TemplateVersionID: source.TemplateVersionID,
	}

	task.RootID = task.ID

	if editAction != "" {
		task.EditAction = &editAction
	}
	if editLabel != "" {
		task.EditLabel = &editLabel
	}

	if err := r.Create(ctx, task); err != nil {
		return nil, err
	}

	return task, nil
}

func (r *taskRepository) GetRootTaskByIDAndUser(
	ctx context.Context,
	taskID int64,
	userID int64,
) (*domain.Task, error) {
	var model entity.TaskModel
	err := r.db.WithContext(ctx).
		Where("id = ?", taskID).
		Where("user_id = ?", userID).
		Where("parent_id IS NULL").
		First(&model).Error
	if err != nil {
		return nil, err
	}

	return &domain.Task{
		ID:                   model.ID,
		UserID:               model.UserID,
		Type:                 model.Type,
		Status:               domain.TaskStatus(model.Status),
		RetryCount:           model.RetryCount,
		InputJSON:            model.InputJSON,
		OutputJSON:           model.OutputJSON,
		ParentID:             model.ParentID,
		WorkflowVersionID:    model.WorkflowVersionID,
		WorkflowDefinitionID: model.WorkflowDefinitionID,
		RootID:               model.RootID,
		SubKey:               model.SubKey,
		ParentNode:           model.ParentNode,
		MapIndex:             model.MapIndex,
		Progress:             model.Progress,

		BaseRunID:  model.BaseRunID,
		ForkedFrom: model.ForkedFrom,
		RunDepth:   model.RunDepth,
		EditAction: model.EditAction,
		EditLabel:  model.EditLabel,
		ResumeFrom: model.ResumeFrom,
		PatchJSON:  model.PatchJSON,

		RouteKey:          model.RouteKey,
		ModeKey:           model.ModeKey,
		EntryType:         model.EntryType,
		EntryTitle:        model.EntryTitle,
		EntrySubtitle:     model.EntrySubtitle,
		ToolDefinitionID:  model.ToolDefinitionID,
		ToolModeID:        model.ToolModeID,
		ToolModeVersionID: model.ToolModeVersionID,
		TemplateID:        model.TemplateID,
		TemplateVersionID: model.TemplateVersionID,

		CreatedAt: model.CreatedAt,
		UpdatedAt: model.UpdatedAt,
	}, nil
}

func (r *taskRepository) ListChildrenByParentID(ctx context.Context, parentID int64) ([]*domain.Task, error) {
	var models []entity.TaskModel

	err := r.db.WithContext(ctx).
		Where("parent_id = ?", parentID).
		Order("id ASC").
		Find(&models).
		Error
	if err != nil {
		return nil, err
	}

	result := make([]*domain.Task, 0, len(models))
	for _, m := range models {
		result = append(result, &domain.Task{
			ID:                   m.ID,
			UserID:               m.UserID,
			Type:                 m.Type,
			Status:               domain.TaskStatus(m.Status),
			RetryCount:           m.RetryCount,
			InputJSON:            m.InputJSON,
			OutputJSON:           m.OutputJSON,
			ErrorMessage:         utils.ValueOrEmpty(m.ErrorMessage),
			ParentID:             m.ParentID,
			WorkflowVersionID:    m.WorkflowVersionID,
			WorkflowDefinitionID: m.WorkflowDefinitionID,
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

	return result, nil
}

// 批量更新更高效
func (r *taskRepository) BatchUpdateStatus(ctx context.Context, taskIDs []int64, status domain.TaskStatus, errMsg string) error {
	if len(taskIDs) == 0 {
		return nil
	}

	update := map[string]any{
		"status":     string(status),
		"updated_at": time.Now(),
	}

	if errMsg != "" {
		update["error_message"] = errMsg
	} else {
		update["error_message"] = nil
	}

	return r.db.WithContext(ctx).
		Model(&entity.TaskModel{}).
		Where("id IN ?", taskIDs).
		Updates(update).Error
}
