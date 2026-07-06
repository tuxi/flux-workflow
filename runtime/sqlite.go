package runtime

import (
	"fmt"
	"github.com/tuxi/flux-workflow/domain/entity"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// openSQLite opens a SQLite database at the given path, enables WAL mode,
// sets a busy timeout, and runs AutoMigrate for all entity tables.
func openSQLite(path string) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}

	if err := db.Exec("PRAGMA journal_mode=WAL;").Error; err != nil {
		return nil, fmt.Errorf("sqlite WAL: %w", err)
	}
	if err := db.Exec("PRAGMA busy_timeout=5000;").Error; err != nil {
		return nil, fmt.Errorf("sqlite busy_timeout: %w", err)
	}

	if err := db.AutoMigrate(
		&entity.TaskModel{},
		&entity.TaskEventModel{},
		&entity.TaskNodeModel{},
		&entity.TaskCostTraceModel{},
		&entity.AwaitBindingModel{},
		&entity.WorkflowModel{},
		&entity.WorkflowVersionModel{},
	); err != nil {
		return nil, fmt.Errorf("sqlite migrate: %w", err)
	}

	return db, nil
}
