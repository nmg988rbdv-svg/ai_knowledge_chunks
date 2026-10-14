package workflow

import (
	"context"
	"strings"
)

// RouteState 是路由图在节点之间传递的共享状态。
type RouteState struct {
	Input      string `json:"input"`
	Normalized string `json:"normalized"`
	Intent     string `json:"intent"`
	Path       string `json:"path"`
	Output     string `json:"output"`
}

type RouteResult struct {
	RouteState
	Visited []string `json:"visited"`
}

// Router 包装编译好的路由图，可并发处理无共享可变状态的请求。
type Router struct {
	graph *Graph[RouteState]
}

// NewRouter 创建并初始化一个新的路由器实例，设置路由图的节点和边。
func NewRouter() *Router {
	graph := NewGraph[RouteState](10)
	must(graph.AddNode("normalize", normalizeNode))
	must(graph.AddNode("classify", classifyNode))
	must(graph.AddNode("chat", routeNode("chat", "route to the normal chat pipeline")))
	must(graph.AddNode("rag", routeNode("rag", "route to the knowledge retrieval pipeline")))
	must(graph.AddNode("agent", routeNode("agent", "route to the tool-using agent pipeline")))
	must(graph.SetEntryPoint("normalize"))
	must(graph.AddEdge("normalize", "classify"))
	must(graph.AddConditionalEdge("classify", routeByIntent))
	must(graph.AddEdge("chat", End))
	must(graph.AddEdge("rag", End))
	must(graph.AddEdge("agent", End))
	return &Router{graph: graph}
}

// Route 根据用户输入在路由图中找到合适的路径，返回最终的路由状态和访问过的节点列表。
func (r *Router) Route(ctx context.Context, input string) (RouteResult, error) {
	// 初始化路由状态，设置用户输入字段。
	state, visited, err := r.graph.Run(ctx, RouteState{Input: input})
	return RouteResult{RouteState: state, Visited: visited}, err
}

// normalizeNode 是路由图中的归一化节点，用于对用户输入进行预处理。
func normalizeNode(_ context.Context, state RouteState) (RouteState, error) {
	// 对用户输入进行预处理，去除前后空格。
	state.Normalized = strings.TrimSpace(state.Input)
	return state, nil
}

// classifyNode 是路由图中的分类节点，根据归一化后的输入确定意图。
func classifyNode(_ context.Context, state RouteState) (RouteState, error) {
	// 将归一化后的输入转换为小写，便于后续意图分类。
	input := strings.ToLower(state.Normalized)
	// 根据输入内容匹配意图关键词，设置意图字段。
	switch {
	case containsAny(input, "知识库", "资料", "文档", "rag", "检索"):
		// 如果匹配到知识库相关的关键词，则设置意图为 "rag"。
		state.Intent = "rag"
	case containsAny(input, "计算", "工具", "agent", "加上", "减去", "乘以", "除以"):
		// 如果匹配到计算或工具相关的关键词，则设置意图为 "agent"。
		state.Intent = "agent"
	default:
		// 如果未匹配到上述关键词，则默认设置意图为 "chat"。
		state.Intent = "chat"
	}
	return state, nil
}

// routeByIntent 是路由图中的条件路由节点，根据意图选择下一个节点。
func routeByIntent(_ context.Context, state RouteState) (string, error) {
	return state.Intent, nil
}

// routeNode 创建一个固定路由节点，设置路径和输出信息。
func routeNode(path, output string) Node[RouteState] {
	return func(_ context.Context, state RouteState) (RouteState, error) {
		state.Path = path
		state.Output = output
		return state, nil
	}
}

// containsAny 检查文本中是否包含任意指定的子字符串。
func containsAny(text string, values ...string) bool {
	for _, value := range values {
		if strings.Contains(text, value) {
			return true
		}
	}
	return false
}

// must 是一个辅助函数，用于在初始化路由图时确保操作成功，否则触发 panic。
func must(err error) {
	if err != nil {
		panic(err)
	}
}
