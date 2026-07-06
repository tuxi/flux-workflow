# CreativeDetailService 设计文档

## 背景

当前 `creative_detail` 由各工作流 DSL 中的 `build_creative_detail` 节点生成，并通过 `definition.Output.CreativeDetail` 写入任务最终 `output_json`。这个设计最初服务于客户端快速展示：客户端只要读取任务 final output，就能拿到完整创意详情。

随着资产存储升级，尤其是资产私有化、短签 URL、权限校验和资产生命周期管理引入后，`creative_detail` 继续存储在 `tasks.output_json` 中会产生结构性冲突：

- `creative_detail` 中的媒体 URL 会过期。
- 私有资产访问必须经过用户权限校验。
- 同一资产在不同访问场景下可能需要不同签名 URL。
- `creative_detail` 是多个节点 output 的展示聚合，不是任务最终结果事实。
- 模板发布、客户端详情页、管理后台详情页都不应该依赖一份静态 JSON 快照。

因此，本次改造的核心不是删除 `creative_detail`，而是重新定义它的业务边界。

## 核心结论

`creative_detail` 不应该被任何表字段存储。

它应该成为一个由 `CreativeDetailService` 动态查询和构建的业务视图：

```text
task input_json
task_nodes output_json / checkpoint_json
workflow definition
creative detail builder spec
        |
        v
CreativeDetailService
        |
        v
domain.CreativeDetail
```

任务表、模板样例表、工作流 final output 都不再存储 `creative_detail`。

## 事实来源

本设计承认并固定一个新的事实：

```text
task_nodes 不只是运行态日志，也是 creative_detail 的事实来源。
```

`creative_detail` 不是独立事实，而是从任务执行轨迹中可再生的展示视图。具体事实来源包括：

- `tasks.input_json`
- `tasks.output_json` 中的最终结果字段
- `task_nodes.output_json`
- `task_nodes.checkpoint_json`
- 工作流 DSL / workflow version
- 各场景已有的 `build_creative_detail` tool 实现

## 非目标

本次改造不做以下事情：

- 不在 `tasks.output_json` 中继续写入 `creative_detail`。
- 不在 `template_samples.creative_detail_json` 或 `creative_narrative_json` 中保存 `creative_detail`。
- 不为每个业务场景重新实现一套创意详情构建逻辑。
- 不要求客户端从 task output 中自行拼装创意详情。

## 服务位置

`CreativeDetailService` 应放在：

```text
ai-engine/service
```

建议文件：

```text
ai-engine/service/creative_detail_service.go
ai-engine/service/creative_detail_service_test.go
```

## 服务接口

建议先定义最小接口：

```go
type CreativeDetailService interface {
    BuildTaskCreativeDetail(ctx context.Context, taskID int64) (*domain.CreativeDetail, error)
}
```

如果模板样例也需要直接查询，可以在上层服务中先通过 `source_task_id` 调用 `BuildTaskCreativeDetail`。后续确有必要时再扩展：

```go
type CreativeDetailService interface {
    BuildTaskCreativeDetail(ctx context.Context, taskID int64) (*domain.CreativeDetail, error)
    BuildTemplateSampleCreativeDetail(ctx context.Context, sampleID int64) (*domain.CreativeDetail, error)
}
```

## 构建流程

`BuildTaskCreativeDetail` 的流程建议如下：

```text
1. 查询 task
2. 校验 task 存在且状态允许展示
3. 查询 workflow version / workflow definition
4. 查询 task_nodes
5. 构建只读 nodes.Context
6. 将 task input 注入 ctx.Output["input"]
7. 将 task_nodes output 注入 ctx.Output["nodes"]
8. 对 map / loop 等节点，必要时从 checkpoint_json 重建 output
9. 获取 workflow 的 creative detail builder spec
10. 使用 Engine.buildNodeInput 或等价的只读 input resolver 解析 builder input
11. 调用已有 ai-engine/tool 下对应的 build_creative_detail tool
12. 对返回的 CreativeDetail 做资产引用水合和短签 URL 处理
13. 返回 CreativeDetail
```

现有 `ai-engine/engine/replay_engine.go` 已经具备类似能力：从持久化的 `task_nodes` 重建只读上下文，并重算节点 resolved input。`CreativeDetailService` 应尽量复用或抽取这部分能力，避免另写一套表达式解析逻辑。

## DSL 改造

当前 DSL 中普遍存在实际执行节点：

```text
build_creative_detail -> end
```

以及 output 定义：

```go
CreativeDetail: "nodes.build_creative_detail.output.creative_detail"
```

新设计下，`build_creative_detail` 不再作为运行时节点参与工作流执行，也不再写入 final output。

但是，不能完全丢掉它的定义。原因是查询时仍然需要知道：

- 使用哪个 tool 构建详情。
- 需要哪些 input。
- 每个 input 从哪个节点 output 表达式取值。
- 是否有静态 config。

因此建议在 workflow definition 中新增一个独立的 detail builder spec，例如：

```go
CreativeDetailBuilder: &definition.CreativeDetailBuilderDefinition{
    Tool: "build_image_to_image_creative_detail",
    Config: map[string]any{
        // 静态配置
    },
    InputMapping: map[string]string{
        "image_url": "nodes.image_cache_save.output.url",
        "width": "nodes.provider_result_merge.output.width",
        "height": "nodes.provider_result_merge.output.height",
    },
}
```

它的语义是：

```text
只用于查询 creative_detail，不参与 DAG 执行。
```

### 重要约束

`CreativeDetailBuilder` 应强制使用显式 `InputMapping`。

不要依赖 `buildNodeInput` 的自动 fallback。因为 `build_creative_detail` 从执行 DAG 中独立后，不再天然拥有稳定的 parent 关系；继续依赖 fallback 会让查询行为变得隐式且脆弱。

## task output 改造

`domain.TaskOutput` 中应逐步移除或废弃：

```go
CreativeDetail *CreativeDetail `json:"creative_detail,omitempty"`
```

新任务 final output 只保留最终结果事实：

```json
{
  "result_type": "image",
  "primary_file_url": "...",
  "preview_url": "...",
  "width": 1440,
  "height": 2560,
  "extras": {}
}
```

进一步配合资产私有化时，`primary_file_url`、`preview_url`、`cover_url`、`extras.image_url` 等字段也应逐步从长期 URL 过渡为资产引用：

```json
{
  "primary_asset_id": 123,
  "primary_oss_key": "images/final/xxx.jpeg"
}
```

URL 应在接口响应层动态签名。

## task_nodes 持久化约束

由于 `task_nodes` 成为 `creative_detail` 的事实来源，所有 detail builder 依赖的节点 output 必须满足以下条件之一：

- 已持久化到 `task_nodes.output_json`。
- 可从 `task_nodes.checkpoint_json` 重建。
- 可从 `tasks.input_json` 或 `tasks.output_json` 稳定取得。

如果某个节点配置了：

```go
persist_output: false
```

但其 output 又被 `CreativeDetailBuilder.InputMapping` 引用，则这是 DSL 错误，应通过测试阻止。

建议为每个 workflow 增加测试：

```text
CreativeDetailBuilder 引用的所有 nodes.xxx.output 路径都来自可持久化或可重建节点。
```

## 管理后台模板发布改造

`taskPublishService` 原先有两处依赖 `out.CreativeDetail`：

```go
buildSuggestedPrimarySampleFromTask(...)
createTemplatePrimarySampleFromTaskTx(...)
```

新设计下，这两处都不再从 `TaskOutput` 读取 creative detail，而是统一通过 `CreativeDetailService` 动态构建。

### 草稿阶段

构建模板发布草稿时：

```text
BuildTemplateDraftFromTask
  -> parse task output 获取结果事实
  -> CreativeDetailService.BuildTaskCreativeDetail(task.ID)
  -> 返回 suggested_primary_sample.creative_detail
```

注意：这里返回给管理后台可以包含 `creative_detail`，但这只是接口响应，不是落库字段。

### 发布阶段

发布模板时，`template_samples` 不再写入 `CreativeDetailJSON`。

模板样例只保留来源关系：

```text
source_type = "task"
source_task_id = task.ID
input_json = task.input_json
output_json = task.output_json
```

模板详情页需要展示样例 creative detail 时：

```text
template_sample.source_task_id
  -> CreativeDetailService.BuildTaskCreativeDetail(source_task_id)
```

### 关于人工覆盖

发布请求 DTO 不再支持管理后台提交 creative detail 覆盖字段。旧设计中的字段为：

```go
CreativeDetail *domain.CreativeDetail `json:"creative_detail,omitempty"`
```

如果坚持 `creative_detail` 不被任何表字段存储，则这个人工覆盖能力也应删除或改造。

可选替代方案：

- 后台只允许编辑 template/sample 的 title、subtitle、description、preview 等元信息。
- 不允许直接编辑 creative_detail。
- 如确实需要编辑，应编辑 detail builder 的来源数据或新增独立的人工注释字段，而不是存储完整 creative_detail。

## 客户端接口改造

客户端不再从任务详情的 `output_json.final.creative_detail` 读取创意详情。

新增或调整接口：

```text
GET /api/v1/tasks/{task_id}/creative-detail
```

返回：

```json
{
  "creative_detail": {}
}
```

接口职责：

- 校验当前用户是否有权访问 task。
- 调用 `CreativeDetailService.BuildTaskCreativeDetail`。
- 对媒体资源做权限校验和短签 URL 水合。
- 返回可展示的 creative detail。

模板样例详情也应通过独立接口或模板详情响应动态返回：

```text
GET /api/v1/templates/{template_id}
GET /api/v1/template-samples/{sample_id}/creative-detail
```

内部都不读取存储的 creative_detail，而是通过 `source_task_id` 动态构建。

## 资产 URL 处理

`CreativeDetailService` 调用 builder 后，不应直接把历史 URL 原样返回给客户端。

建议在 detail 结果上增加统一的资源水合步骤：

```text
CreativeDetail asset refs
  -> permission check
  -> sign URL
  -> response URL / preview_url / cover_url
```

长期目标是让 build tool 输出资产引用，而非长期 URL：

```json
{
  "type": "image",
  "asset_id": 123,
  "oss_key": "images/final/xxx.jpeg",
  "role": "result_media"
}
```

短期如果旧节点 output 里只有 URL，可以先通过 OSS URL 反解 `oss_key`，再查询资产表补齐。

## 兼容策略

由于系统尚未上线，可以采用彻底切换：

- 新任务不再写入 `creative_detail`。
- DSL 移除运行时 `build_creative_detail` 节点。
- DSL 新增 `CreativeDetailBuilder` spec。
- 管理后台发布模板不再持久化 `creative_detail`。
- 客户端和后台统一调用 creative detail 查询接口。

仍建议保留短期读取旧 `TaskOutput.CreativeDetail` 的兼容逻辑，方便本地已有任务和测试数据过渡，但不作为新数据路径。

## 需要改造的模块

### ai-engine/definition

- 新增 `CreativeDetailBuilderDefinition`。
- 从 `OutputDefinition` 中废弃 `CreativeDetail`。

### ai-engine/workflows

- 移除各 DSL 中运行时 `build_creative_detail` 节点和相关 edge。
- 将原节点的 tool/config/input_mapping 迁移到 `CreativeDetailBuilder`。
- 补充 DSL 测试，确保 builder input mapping 可解析且依赖节点可持久化。

### ai-engine/engine

- 抽取只读 replay context 构建能力，供 `CreativeDetailService` 复用。
- 提供面向 detail builder 的 input resolver。
- 避免查询 creative detail 时触发真实节点执行或外部 API 调用。

### ai-engine/service

- 新增 `CreativeDetailService`。
- 负责 task 查询、workflow 查询、task_nodes replay、builder input 构建、tool 执行、资产水合。

### ai-engine/handler

- 新增任务 creative detail 查询接口。
- 任务详情响应不再依赖 `output_json.final.creative_detail`。

### internal/service/task_publish.go

- `BuildTemplateDraftFromTask` 改为调用 `CreativeDetailService` 生成草稿响应中的 suggested primary sample detail。
- `createTemplatePrimarySampleFromTaskTx` 不再写入 `CreativeDetailJSON`。
- 发布后的模板 sample 通过 `source_task_id` 动态构建 creative detail。

### internal/model/entity/template_sample.go

- 废弃 `CreativeDetailJSON` / `creative_narrative_json` 字段。
- 保留 `SourceTaskID` 作为动态构建来源。

### internal/model/dto

- 删除发布请求中的 `primary_sample.creative_detail` 覆盖字段，或标记废弃。
- 响应 DTO 可以继续返回 `creative_detail`，但它来自服务动态构建。

## 风险与约束

### 历史展示会随代码变化

因为 creative detail 每次动态构建，builder tool 的代码变更可能影响历史任务的展示结果。

如果需要稳定历史展示，应通过 builder version 或 workflow version 固定对应实现，而不是存储 creative_detail 快照。

### task_nodes 生命周期变长

模板样例如果通过 `source_task_id` 动态构建 creative detail，则源 task 及其 task_nodes 不能随意清理。

需要制定保留策略：

- 被模板引用的 task 标记为 protected。
- 被引用 task 的 task_nodes 不参与普通日志清理。
- 如需归档，必须连同 task_nodes 一起归档，并保证可查询。

### 查询成本增加

每次查询都要加载 task、workflow、task_nodes 并执行 builder tool。

可以后续增加短 TTL 缓存，但缓存不应成为事实来源。

```text
cache key = task_id + workflow_version_id + task_nodes updated_at/hash
```

## 推荐落地顺序

1. 定义 `CreativeDetailBuilderDefinition`。
2. 实现 `CreativeDetailService.BuildTaskCreativeDetail` 的最小版本。
3. 选择一个图片工作流做试点，例如 image_to_image。
4. 将该 DSL 的 `build_creative_detail` 运行节点迁移为 builder spec。
5. 新增任务 creative detail 查询接口。
6. 修改客户端任务详情读取路径。
7. 修改 `taskPublishService`，去掉 `out.CreativeDetail` 依赖。
8. 批量迁移其他图片、视频、商品工作流 DSL。
9. 删除或废弃 task/template sample 中的 creative detail 存储字段。
10. 增加 DSL 校验测试和 task_nodes 保留策略。

## 最终目标

最终系统应满足：

```text
creative_detail 不落库。
creative_detail 不在 task output 中。
creative_detail 由 CreativeDetailService 动态构建。
task_nodes 是 creative_detail 的事实来源。
各场景继续复用已有 build_creative_detail tool。
客户端、管理后台、模板样例统一走服务查询。
资产 URL 在响应时动态签名。
```
