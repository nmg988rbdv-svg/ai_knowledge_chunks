package workflow

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type counterState struct {
	Count int
}

func TestGraphSupportsConditionalLoop(t *testing.T) {
	graph := NewGraph[counterState](10)
	if err := graph.AddNode("increment", func(_ context.Context, state counterState) (counterState, error) {
		state.Count++
		return state, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := graph.SetEntryPoint("increment"); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddConditionalEdge("increment", func(_ context.Context, state counterState) (string, error) {
		if state.Count < 3 {
			return "increment", nil
		}
		return End, nil
	}); err != nil {
		t.Fatal(err)
	}

	state, visited, err := graph.Run(context.Background(), counterState{})
	if err != nil {
		t.Fatalf("run graph: %v", err)
	}
	if state.Count != 3 || !reflect.DeepEqual(visited, []string{"increment", "increment", "increment"}) {
		t.Fatalf("state=%#v visited=%#v", state, visited)
	}
}

func TestGraphStopsInfiniteLoopAtStepLimit(t *testing.T) {
	graph := NewGraph[counterState](2)
	if err := graph.AddNode("loop", func(_ context.Context, state counterState) (counterState, error) {
		state.Count++
		return state, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := graph.SetEntryPoint("loop"); err != nil {
		t.Fatal(err)
	}
	if err := graph.AddEdge("loop", "loop"); err != nil {
		t.Fatal(err)
	}

	state, visited, err := graph.Run(context.Background(), counterState{})
	if !errors.Is(err, ErrStepLimit) {
		t.Fatalf("error=%v, want ErrStepLimit", err)
	}
	if state.Count != 2 || len(visited) != 2 {
		t.Fatalf("state=%#v visited=%#v", state, visited)
	}
}

func TestRoutingGraphSelectsExpectedPath(t *testing.T) {
	router := NewRouter()
	tests := []struct {
		input string
		path  string
	}{
		{input: "你好，请介绍一下 Go", path: "chat"},
		{input: "从知识库检索退款规则", path: "rag"},
		{input: "计算 12 乘以 7", path: "agent"},
	}
	for _, test := range tests {
		result, err := router.Route(context.Background(), test.input)
		if err != nil {
			t.Fatalf("route %q: %v", test.input, err)
		}
		if result.Path != test.path || result.Intent != test.path {
			t.Fatalf("input=%q result=%#v", test.input, result)
		}
		wantVisited := []string{"normalize", "classify", test.path}
		if !reflect.DeepEqual(result.Visited, wantVisited) {
			t.Fatalf("input=%q visited=%#v", test.input, result.Visited)
		}
	}
}
