package nodes

import (
	"github.com/tuxi/flux-workflow/tool"
)

// RegisterBuiltinNodes registers the 6 built-in node types that every
// workflow engine needs: start, end, subworkflow, map, loop, await.
//
// The caller must also register tool nodes via:
//
//	reg.Register(NewToolFactory(toolReg), ToolNodeSchema)
//
// with their own tool.Registry.
func RegisterBuiltinNodes(reg *NodeRegistry) {
	reg.Register(NewStartNodeFactory(), StartNodeSchema)
	reg.Register(NewEndNodeFactory(), EndNodeSchema)
	reg.Register(NewSubWorkflowNodeFactory(), SubWorkflowNodeSchema)
	reg.Register(NewMapNodeFactory(), MapNodeSchema)
	reg.Register(NewLoopNodeFactory(), LoopNodeSchema)
	reg.Register(NewAwaitNodeFactory(), AwaitNodeSchema)
}

// InitNodeRegistry creates a new NodeRegistry, registers built-in nodes
// and all tools from the given tool.Registry, then returns the registry.
//
// Deprecated: use NewNodeRegistry + RegisterBuiltinNodes + explicit tool
// node registration instead. This function uses a package-level global
// and prevents multiple independent registries in the same process.
func InitNodeRegistry(toolReg *tool.Registry) *NodeRegistry {
	reg = NewNodeRegistry()
	RegisterBuiltinNodes(reg)
	reg.Register(NewToolFactory(toolReg), ToolNodeSchema)
	return reg
}
