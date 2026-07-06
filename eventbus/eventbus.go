package eventbus

import (
	"context"
	"flux-workflow/domain"
	"flux-workflow/repository"
	"strings"
	"sync"
)

// EventBus — AI Engine Event Infrastructure V2
//
// 事件分层路由：
//   - Transient  → WS only（不入库，不 replay，丢失没关系）
//   - Persistent → DB + WS + Sequence（状态恢复、Timeline 重建）
//   - Audit      → DB only（不推送 WS）
//
// Phase 2 roadmap:
//   - timeline_patch: PatchImportance tiers (structural/visual/ephemeral), only structural persists
//   - Client: Snapshot + Incremental Events instead of full replay
//   - inferGrade: remove fallback, all emitters set Grade explicitly
type EventBus struct {
	eventRepo repository.EventRepository
	publisher Publisher

	subscribers map[string][]chan *domain.TaskEvent
	mu          sync.RWMutex
}

func NewEventBus(
	repo repository.EventRepository,
	pub Publisher,
) *EventBus {

	return &EventBus{
		eventRepo:   repo,
		publisher:   pub,
		subscribers: make(map[string][]chan *domain.TaskEvent),
	}
}

// Publish 按事件等级分流：
//   - Transient: 仅 push WS
//   - Persistent: 写 DB（分配 sequence）+ push WS
//   - Audit: 仅写 DB
//
// 如果 event.Grade 为空，则通过 inferGrade 自动推断。
func (b *EventBus) Publish(taskID int64, event *domain.TaskEvent) {
	if event.TaskID == 0 {
		event.TaskID = taskID
	}

	// 自动推断等级（兼容未显式设置 Grade 的调用方）
	if event.Grade == "" {
		event.Grade = inferGrade(event.Type)
	}

	// 1. 持久化：Persistent / Audit
	if b.eventRepo != nil && event.Grade == domain.GradePersistent {
		_ = b.eventRepo.Create(context.Background(), event)
	}
	if b.eventRepo != nil && event.Grade == domain.GradeAudit {
		_ = b.eventRepo.Create(context.Background(), event)
	}

	// 2. 推送 WS：Transient / Persistent
	if b.publisher != nil && event.Grade != domain.GradeAudit {
		b.publisher.Publish(TaskChannel(taskID), event)
	}

	// 3. 内部订阅（所有等级都分发，引擎监听需要）
	// 发送必须在锁内进行：Unsubscribe 先持写锁摘除 channel 再 close，
	// 锁内发送保证不会向已 close 的 channel 发送。
	b.mu.RLock()
	for _, ch := range b.subscribers[event.Type] {
		select {
		case ch <- event:
		default:
		}
	}
	b.mu.RUnlock()
}

// PublishToChannel 直接向指定 WS channel 推送事件，不写 DB。
// 专用于回放等只需 WS 输出的场景。
func (b *EventBus) PublishToChannel(ch Channel, event *domain.TaskEvent) {
	if b.publisher != nil {
		b.publisher.Publish(ch, event)
	}
}

// inferGrade is a fallback for callers that don't set Grade explicitly.
// New emitters should set event.Grade directly — do NOT rely on string matching.
// Phase 2: remove this fallback once all emitters set Grade explicitly.
func inferGrade(eventType string) domain.EventGrade {
	// Transient: 实时流事件 — 只用于 UI 反馈，不入库
	for _, prefix := range []string{
		"tool_stream",
		"tool_stream_end",
		"tool_progress",
		"tool_log",
		"node_debug",
		"task_progress", // 任务进度通过 tasks.progress 字段持久化，事件本身只是 WS 实时推送
	} {
		if strings.HasPrefix(eventType, prefix) {
			return domain.GradeTransient
		}
	}

	// Audit 等级不再靠事件名推断：需要只入库不推送的调用方（如计费审计）
	// 应在发布时显式设置 event.Grade = GradeAudit。
	return domain.GradePersistent
}

func (b *EventBus) Subscribe(eventType string) <-chan *domain.TaskEvent {

	ch := make(chan *domain.TaskEvent, 100)

	b.mu.Lock()
	b.subscribers[eventType] = append(b.subscribers[eventType], ch)
	b.mu.Unlock()

	return ch
}

// Unsubscribe 摘除订阅并关闭 channel，令消费方的 range 循环退出。
// 对未订阅的 channel 调用是安全的 no-op。
func (b *EventBus) Unsubscribe(eventType string, ch <-chan *domain.TaskEvent) {
	var closed chan *domain.TaskEvent

	b.mu.Lock()
	subs := b.subscribers[eventType]
	for i, sub := range subs {
		if sub == ch {
			b.subscribers[eventType] = append(subs[:i], subs[i+1:]...)
			closed = sub
			break
		}
	}
	b.mu.Unlock()

	// 摘除后不会再有 Publish 引用它（发送在锁内），此时 close 安全
	if closed != nil {
		close(closed)
	}
}
