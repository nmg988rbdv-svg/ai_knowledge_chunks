package workflow

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const End = "__end__"

var ErrStepLimit = errors.New("workflow step limit exceeded")

// Node 读取当前状态并返回更新后的状态。
type Node[S any] func(context.Context, S) (S, error)

// RouteFunc 根据节点执行后的状态选择下一个节点。
type RouteFunc[S any] func(context.Context, S) (string, error)

// Graph 是轻量级 StateGraph：节点负责处理状态，边负责控制流程。
type Graph[S any] struct {
	nodes    map[string]Node[S]
	edges    map[string]string
	routers  map[string]RouteFunc[S]
	entry    string
	maxSteps int
}

func NewGraph[S any](maxSteps int) *Graph[S] {
	if maxSteps <= 0 {
		maxSteps = 20
	}
	return &Graph[S]{
		nodes:    make(map[string]Node[S]),
		edges:    make(map[string]string),
		routers:  make(map[string]RouteFunc[S]),
		maxSteps: maxSteps,
	}
}

// AddNode 向图中添加一个新的节点。
func (g *Graph[S]) AddNode(name string, node Node[S]) error {
	// 去除节点名称的前后空格。
	name = strings.TrimSpace(name)
	// 检查节点名称是否为空、是否为保留名称或处理函数是否为 nil。
	if name == "" || name == End || node == nil {
		return errors.New("node must have a non-empty non-reserved name and function")
	}
	// 检查节点是否已存在。
	if _, exists := g.nodes[name]; exists {
		return fmt.Errorf("node %q already exists", name)
	}
	g.nodes[name] = node
	return nil
}

// SetEntryPoint 设置图的入口节点。
func (g *Graph[S]) SetEntryPoint(name string) error {
	if _, exists := g.nodes[name]; !exists {
		return fmt.Errorf("entry node %q does not exist", name)
	}
	g.entry = name
	return nil
}

// AddEdge 向图中添加一条固定边，从源节点指向目标节点。
func (g *Graph[S]) AddEdge(from, to string) error {
	if err := g.validateEndpoints(from, to); err != nil {
		return err
	}
	if _, exists := g.routers[from]; exists {
		return fmt.Errorf("node %q already uses conditional routing", from)
	}
	if _, exists := g.edges[from]; exists {
		return fmt.Errorf("node %q already has a fixed edge", from)
	}
	g.edges[from] = to
	return nil
}

// AddConditionalEdge 向图中添加一条条件边，根据路由函数动态选择下一个节点。
func (g *Graph[S]) AddConditionalEdge(from string, router RouteFunc[S]) error {
	if _, exists := g.nodes[from]; !exists {
		return fmt.Errorf("source node %q does not exist", from)
	}
	if router == nil {
		return errors.New("conditional router must not be nil")
	}
	if _, exists := g.edges[from]; exists {
		return fmt.Errorf("node %q already uses fixed routing", from)
	}
	if _, exists := g.routers[from]; exists {
		return fmt.Errorf("node %q already has a conditional edge", from)
	}
	g.routers[from] = router
	return nil
}

// Run 从入口开始执行，并返回最终状态和按顺序访问过的节点。
// 如果超过最大步数限制，将返回 ErrStepLimit。
func (g *Graph[S]) Run(ctx context.Context, state S) (S, []string, error) {

	// 检查是否已配置入口节点。
	if g.entry == "" {
		return state, nil, errors.New("workflow entry point is not configured")
	}

	// 初始化当前节点为入口节点，并准备记录访问过的节点。
	current := g.entry
	// visited 用于记录按顺序访问过的节点。
	visited := make([]string, 0, g.maxSteps)
	// 遍历节点，直到达到最大步数或遇到结束节点。
	for step := 0; step < g.maxSteps; step++ {

		// 检查上下文是否已被取消或超时。
		if err := ctx.Err(); err != nil {
			return state, visited, err
		}
		// 获取当前节点的处理函数。
		node, exists := g.nodes[current]
		if !exists {
			return state, visited, fmt.Errorf("node %q does not exist", current)
		}
		// 执行当前节点的处理函数。
		updated, err := node(ctx, state)
		if err != nil {
			return state, visited, fmt.Errorf("execute node %q: %w", current, err)
		}
		// 更新状态并记录访问过的节点。
		state = updated
		visited = append(visited, current)

		// 确定下一个节点。
		next, err := g.nextNode(ctx, current, state)
		if err != nil {
			return state, visited, err
		}
		// 如果到达结束节点，则终止执行。
		if next == End {
			return state, visited, nil
		}
		// 更新当前节点为下一个节点。
		current = next
	}
	return state, visited, ErrStepLimit
}

// nextNode 根据当前节点和状态确定下一个节点。优先使用条件路由，其次使用固定边。
func (g *Graph[S]) nextNode(ctx context.Context, current string, state S) (string, error) {
	// 优先使用条件路由确定下一个节点。
	if router, exists := g.routers[current]; exists {
		// 调用条件路由函数获取下一个节点。
		next, err := router(ctx, state)
		if err != nil {
			return "", fmt.Errorf("route from node %q: %w", current, err)
		}
		// 如果路由函数返回的是结束节点，直接返回结束。
		if next == End {
			return End, nil
		}
		// 检查路由函数返回的节点是否存在于图中。
		if _, exists := g.nodes[next]; !exists {
			return "", fmt.Errorf("router from node %q selected unknown node %q", current, next)
		}
		return next, nil
	}

	next, exists := g.edges[current]
	if !exists {
		return "", fmt.Errorf("node %q has no outgoing edge", current)
	}
	return next, nil
}

func (g *Graph[S]) validateEndpoints(from, to string) error {
	if _, exists := g.nodes[from]; !exists {
		return fmt.Errorf("source node %q does not exist", from)
	}
	if to == End {
		return nil
	}
	if _, exists := g.nodes[to]; !exists {
		return fmt.Errorf("target node %q does not exist", to)
	}
	return nil
}
