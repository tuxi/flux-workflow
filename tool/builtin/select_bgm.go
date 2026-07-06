package builtin

import (
	"context"
	"github.com/tuxi/flux/tool"
)

type SelectBGMTool struct{}

func NewSelectBGMTool() *SelectBGMTool {
	return &SelectBGMTool{}
}

func (t *SelectBGMTool) Name() string {
	return "select_bgm"
}

func (t *SelectBGMTool) Description() string {
	return "Select background music"
}

func (t *SelectBGMTool) InputSchema() tool.DataSchema {
	return tool.DataSchema{
		Fields: map[string]tool.FieldSchema{
			"bgm_files": {
				Type: "array",
				Desc: "BGM 文件列表",
			},
		},
	}
}

func (t *SelectBGMTool) OutputSchema() tool.DataSchema {
	return tool.DataSchema{
		Fields: map[string]tool.FieldSchema{
			"audio": {
				Type: "string",
				Desc: "选中的 BGM 文件",
			},
		},
	}
}

func (t *SelectBGMTool) Execute(
	ctx context.Context,
	input map[string]any,
	emitter tool.ToolEmitter,
) (*tool.Result, error) {

	files, ok := input["bgm_files"].([]any)
	if !ok || len(files) == 0 {
		return &tool.Result{
			Data: map[string]any{
				"audio": "",
			},
		}, nil
	}

	selected := files[0]

	return &tool.Result{
		Data: map[string]any{
			"audio": selected,
		},
	}, nil
}

func (t *SelectBGMTool) Mode() tool.ExecutionMode {
	return tool.SyncExecution
}
