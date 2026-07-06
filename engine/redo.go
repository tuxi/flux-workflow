package engine

import (
	"context"
	"github.com/tuxi/flux-workflow/domain"
)

type RunRedoService interface {
	RedoRun(
		ctx context.Context,
		sourceTaskID int64,
		resumeSpec *domain.ResumeSpec,
		overrideInput map[string]any,
		editAction string,
		editLabel string,
		note string,
	) (*domain.Task, error)
}
