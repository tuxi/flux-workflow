package definition

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/tuxi/flux/utils"
)

// WorkflowDefinition 表示一个“工作流产品”——描述 DAG 拓扑、输入输出契约
// 和下游消费视图的完整模板。它是 Flux 引擎的可执行蓝图。
//
// 顶层字段：
//
//	Name         — 工作流名称（唯一标识）
//	Desc         — 人类可读描述
//	Nodes        — DAG 节点列表（start / llm / video / end 等）
//	Edges        — DAG 有向边列表（from → to，支持条件分支）
//	Output       — 主输出映射（result_type、primary_file_url 等表达式字段）
//	OutputSlices — 可选：下游业务视角的“切片”构建器（creative_detail、timeline 等）
//	              这些切片不参与 DAG 执行，由各自 Service 从已完成节点动态再生数据
//
// 示例 JSON：
//
//	{
//	  "name": "text_to_video",
//	  "description": "从文本生成视频",
//	  "nodes": [
//	    { "name": "start",         "type": "start" },
//	    { "name": "script_gen",    "type": "llm" },
//	    { "name": "video_gen",     "type": "video" },
//	    { "name": "upload_result", "type": "upload" },
//	    { "name": "end",           "type": "end" }
//	  ],
//	  "edges": [
//	    { "from": "start",          "to": "script_gen" },
//	    { "from": "script_gen",     "to": "video_gen" },
//	    { "from": "video_gen",      "to": "upload_result" },
//	    { "from": "upload_result",  "to": "end" }
//	  ],
//	  "output": {
//	    "result_type": "video",
//	    "primary_file_url": "nodes.upload_result.output.url",
//	    "cover_url": "nodes.video_gen.output.cover_url"
//	  },
//	  "output_slices": {
//	    "creative_detail": {
//	      "tool": "creative_detail_builder",
//	      "input_mapping": {
//	        "workflow_id": "nodes.start.input.workflow_id"
//	      }
//	    },
//	    "timeline": {
//	      "tool": "timeline_builder",
//	      "input_mapping": {
//	        "video_url": "nodes.upload_result.output.url"
//	      }
//	    }
//	  }
//	}

// OutputDefinition 定义了如何从节点中提取数据来填充最终的 WorkflowOutput
type OutputDefinition struct {
	ResultType     string `json:"result_type"`      // 固定值，如 "video"
	PrimaryFileUrl string `json:"primary_file_url"` // 表达式，如 "nodes.upload_result.output.url"
	CoverUrl       string `json:"cover_url"`        // 表达式
	PreviewUrl     string `json:"preview_url"`      // 表达式
	Width          string `json:"width"`            // 表达式
	Height         string `json:"height"`           // 表达式
	Duration       string `json:"duration"`         // 表达式

	// 允许通过表达式注入一些业务自定义字段
	Extras map[string]string `json:"extras"` // Value 也是表达式
}

// OutputSliceDefinition 定义工作流产出的一个「切片/视图」构建器。
//
// 每个切片代表一种业务视角的输出再加工——它们不参与 DAG 执行，
// 只由各自的业务 Service 从已完成的 task_nodes 动态再生数据：
//   - creative_detail → 创意详情（CreativeDetailService）
//   - timeline        → 视频时间轴（VideoTimelineService）
//   - 将来可扩展：subtitles（字幕轨道）、thumbnail_strip（缩略图故事板）等
//
// OutputSlice 是工作流定义对"输出还能怎么被消费"的开放式扩展点——
// 新增切片只需在 output_slices map 中添加一个 key，无需修改 WorkflowDefinition 结构体。
type OutputSliceDefinition struct {
	Tool         string            `json:"tool"`
	Config       map[string]any    `json:"config,omitempty"`
	InputMapping map[string]string `json:"input_mapping,omitempty"`
	Version      string            `json:"version,omitempty"`
}

type WorkflowDefinition struct {
	Name         string                            `json:"name"`
	Nodes        []NodeDefinition                  `json:"nodes"`
	Edges        []EdgeDefinition                  `json:"edges"`
	Output       OutputDefinition                  `json:"output"`
	OutputSlices map[string]*OutputSliceDefinition `json:"output_slices,omitempty"`
	Desc         string                            `json:"description"`
}

func (def *WorkflowDefinition) Hash() string {
	hash := hashWorkflow(def)
	return hash
}

type workflowHashStruct struct {
	Name         string                            `json:"name"`
	Desc         string                            `json:"desc,omitempty"`
	Output       OutputDefinition                  `json:"output,omitempty"`
	OutputSlices map[string]*outputSliceHashStruct `json:"output_slices,omitempty"`
	Nodes        []nodeHashStruct                  `json:"nodes"`
	Edges        []edgeHashStruct                  `json:"edges"`
}

type outputSliceHashStruct struct {
	Tool         string            `json:"tool"`
	Config       map[string]any    `json:"config,omitempty"`
	InputMapping map[string]string `json:"input_mapping,omitempty"`
	Version      string            `json:"version,omitempty"`
}

type nodeHashStruct struct {
	Name         string            `json:"name"`
	Type         string            `json:"type"`
	Weight       float64           `json:"weight"`
	Config       map[string]any    `json:"config,omitempty"`
	InputMapping map[string]string `json:"input_mapping,omitempty"`
}

type edgeHashStruct struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Type      string `json:"type"`
	Condition string `json:"condition,omitempty"`
	CaseKey   string `json:"case_key,omitempty"`
	Priority  int    `json:"priority,omitempty"`
	Label     string `json:"label,omitempty"`
}

// HashWorkflow Workflow Hash 算法
// 只要影响执行语义的字段变化，hash 就必须变化
func hashWorkflow(def *WorkflowDefinition) string {
	hashStruct := workflowHashStruct{
		Name: def.Name,
		Desc: def.Desc,
		// 使用克隆方法
		Output:       def.Output.cloneOutputDefinition(),
		OutputSlices: cloneOutputSlicesHash(def.OutputSlices),
		Nodes:        make([]nodeHashStruct, 0, len(def.Nodes)),
		Edges:        make([]edgeHashStruct, 0, len(def.Edges)),
	}

	for _, node := range def.Nodes {
		hashStruct.Nodes = append(hashStruct.Nodes, nodeHashStruct{
			Name:         node.Name,
			Type:         string(node.Type),
			Weight:       node.Weight,
			Config:       utils.NormalizeAnyMap(node.Config),
			InputMapping: utils.CloneStringMap(node.InputMapping),
		})
	}

	for _, edge := range def.Edges {
		hashStruct.Edges = append(hashStruct.Edges, edgeHashStruct{
			From:      edge.From,
			To:        edge.To,
			Type:      string(edge.Type),
			Condition: edge.Condition,
			CaseKey:   edge.CaseKey,
			Priority:  edge.Priority,
			Label:     edge.Label,
		})
	}

	// 保证顺序稳定
	sort.Slice(hashStruct.Nodes, func(i, j int) bool {
		return hashStruct.Nodes[i].Name < hashStruct.Nodes[j].Name
	})

	sort.Slice(hashStruct.Edges, func(i, j int) bool {
		a := hashStruct.Edges[i]
		b := hashStruct.Edges[j]

		if a.From != b.From {
			return a.From < b.From
		}
		if a.To != b.To {
			return a.To < b.To
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		if a.Condition != b.Condition {
			return a.Condition < b.Condition
		}
		if a.CaseKey != b.CaseKey {
			return a.CaseKey < b.CaseKey
		}
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		return a.Label < b.Label
	})

	js, _ := json.Marshal(hashStruct)
	sum := sha256.Sum256(js)
	return hex.EncodeToString(sum[:])
}

// cloneOutputDefinition 深度克隆 OutputDefinition 结构体
func (src OutputDefinition) cloneOutputDefinition() OutputDefinition {
	dst := src
	// Extras 是 map，属于引用类型，必须执行深拷贝
	if src.Extras != nil {
		dst.Extras = make(map[string]string, len(src.Extras))
		for k, v := range src.Extras {
			dst.Extras[k] = v
		}
	}
	return dst
}

func cloneOutputSlicesHash(src map[string]*OutputSliceDefinition) map[string]*outputSliceHashStruct {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]*outputSliceHashStruct, len(src))
	for k, v := range src {
		if v == nil {
			continue
		}
		dst[k] = &outputSliceHashStruct{
			Tool:         v.Tool,
			Config:       utils.NormalizeAnyMap(v.Config),
			InputMapping: utils.CloneStringMap(v.InputMapping),
			Version:      v.Version,
		}
	}
	return dst
}
