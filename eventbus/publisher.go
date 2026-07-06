package eventbus

import "fmt"

// Channel 标识一条实时推送通道（如 "task:123"）。
type Channel string

// Publisher 是实时事件推送的传输抽象。EventBus 只依赖此接口，
// 具体实现（WebSocket、SSE、日志等）由业务侧注入。传输层实现只需
// 提供一个 Publish 方法即可满足该接口。
type Publisher interface {
	Publish(ch Channel, event any)
}

// TaskChannel 返回某个任务的实时事件通道。
func TaskChannel(taskID int64) Channel {
	return Channel(fmt.Sprintf("task:%d", taskID))
}
