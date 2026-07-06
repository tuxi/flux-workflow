package database

import (
	"fmt"
	"github.com/tuxi/flux-workflow/domain/entity"
	"github.com/tuxi/flux-workflow/internal/config"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Database interface {
	DB() *gorm.DB
	CloseDB()
}

type defaultDatabase struct {
	db *gorm.DB
}

func NewDatabase(cfg *config.Database) (Database, error) {
	db, err := connect(cfg)
	if err != nil {
		return nil, err
	}
	return &defaultDatabase{db: db}, nil
}

func (d *defaultDatabase) DB() *gorm.DB {
	if d.db == nil {
		panic("数据库没有初始化，请先初始化它")
	}
	return d.db
}

func (d *defaultDatabase) CloseDB() {
	// 关闭主数据库
	if d.db != nil {
		db, _ := d.db.DB()
		if db != nil {
			_ = db.Close()
		}
	}
}

// connect 初始化并连接数据库
func connect(cfg *config.Database) (*gorm.DB, error) {
	switch cfg.Driver {
	case "sqlite":
		return connectSQLite(cfg)
	default:
		return connectPostgres(cfg)
	}
}

func connectSQLite(cfg *config.Database) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(cfg.DBName), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("sqlite 数据库连接失败: %w", err)
	}

	// 开启 WAL 模式，允许并发读写
	if err := db.Exec("PRAGMA journal_mode=WAL;").Error; err != nil {
		return nil, fmt.Errorf("sqlite 开启 WAL 模式失败: %w", err)
	}
	// 设置 busy timeout，避免写冲突直接报错
	if err := db.Exec("PRAGMA busy_timeout=5000;").Error; err != nil {
		return nil, fmt.Errorf("sqlite 设置 busy timeout 失败: %w", err)
	}

	// AutoMigrate
	if err := db.AutoMigrate(
		&entity.TaskModel{},
		&entity.TaskEventModel{},
		&entity.TaskNodeModel{},
		&entity.TaskCostTraceModel{},
		&entity.AwaitBindingModel{},
		&entity.WorkflowModel{},
		&entity.WorkflowVersionModel{},
	); err != nil {
		return nil, fmt.Errorf("数据库迁移失败: %w", err)
	}

	fmt.Println("✅ SQLite 数据库初始化完成")
	return db, nil
}

func connectPostgres(cfg *config.Database) (*gorm.DB, error) {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%v sslmode=%s TimeZone=Asia/Shanghai",
		cfg.Host, cfg.User, cfg.Password, cfg.DBName, cfg.Port, cfg.SSLMode)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("数据库连接失败: %w", err)
	}

	// 获取底层 SQL 句柄，配置连接池
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	// 设置最大的连接数
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// 1. 初始化必备扩展 (针对 UUID 和 RAG 向量)
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";").Error; err != nil {
		return nil, fmt.Errorf("无法加载 uuid 扩展: %w", err)
	}
	if err := db.Exec("CREATE EXTENSION IF NOT EXISTS vector;").Error; err != nil {
		fmt.Println("⚠️ 警告: vector 扩展加载失败，RAG 功能将受限")
	}

	// 2.自动迁移表结构
	err = db.AutoMigrate(
		&entity.TaskModel{},
		&entity.TaskEventModel{},
		&entity.TaskNodeModel{},
		&entity.TaskCostTraceModel{},
		&entity.AwaitBindingModel{},
		&entity.WorkflowModel{},
		&entity.WorkflowVersionModel{},
	)
	if err != nil {
		return nil, fmt.Errorf("数据库迁移失败: %w", err)
	}

	fmt.Println("✅ dream-AI 数据库初始化完成")
	return db, nil
}
