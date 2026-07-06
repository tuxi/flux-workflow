package websocket

import (
	"fmt"

	"github.com/tuxi/flux-workflow/eventbus"
)

// Channel 复用 eventbus 的通道类型，使 WSHub 天然满足 eventbus.Publisher，
// 无需在核心（eventbus/engine）中反向依赖 websocket。
type Channel = eventbus.Channel

// TaskChannel 转发到 eventbus，保留业务侧既有调用点。
func TaskChannel(taskID int64) Channel {
	return eventbus.TaskChannel(taskID)
}

// WSHub 满足 eventbus.Publisher，可直接注入 EventBus 作为传输实现。
var _ eventbus.Publisher = (*WSHub)(nil)

// ConversationChannel is the room for an Agent conversation's live message feed.
func ConversationChannel(conversationID int64) Channel {
	return Channel(fmt.Sprintf("conversation:%d", conversationID))
}

func WorkflowChannel(id int64) Channel {
	return Channel(fmt.Sprintf("workflow:%d", id))
}

func WorkerChannel(id string) Channel {
	return Channel("worker:" + id)
}

func QueueChannel(name string) Channel {
	return Channel("queue:" + name)
}

func LogsChannel(taskID int64) Channel {
	return Channel(fmt.Sprintf("logs:%d", taskID))
}
