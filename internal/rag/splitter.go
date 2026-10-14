package rag

import (
	"errors"
	"strings"

	"github.com/tmc/langchaingo/textsplitter"
)

// Splitter 允许按内容类型替换切块策略。
type Splitter interface {
	Split(text string) ([]string, error)
}

// LangChainSplitter 使用 LangChainGo 的递归字符切分器，优先保留段落和句子边界。
type LangChainSplitter struct {
	splitter textsplitter.RecursiveCharacter
}

// NewLangChainSplitter 创建一个基于 LangChainGo 的递归字符切分器的实例。
func NewLangChainSplitter(chunkSize, overlap int) (*LangChainSplitter, error) {

	// 校验切块参数的合法性。
	if chunkSize <= 0 || overlap < 0 || overlap >= chunkSize {
		return nil, errors.New("invalid chunk size or overlap")
	}

	// 创建 LangChainGo 的递归字符切分器实例。
	return &LangChainSplitter{splitter: textsplitter.NewRecursiveCharacter(
		textsplitter.WithChunkSize(chunkSize),  // 设置每个切块的最大字符数
		textsplitter.WithChunkOverlap(overlap), // 设置切块的重叠字符数
		textsplitter.WithKeepSeparator(true),   // 保留切块分隔符
	)}, nil
}

// Split 使用 LangChainGo 的递归字符切分器将文本切分为多个块，去除首尾空白并过滤空块。
func (s *LangChainSplitter) Split(text string) ([]string, error) {
	// 去除文本首尾空白，并调用递归字符切分器进行切分。
	parts, err := s.splitter.SplitText(strings.TrimSpace(text))
	if err != nil {
		return nil, err
	}
	// 初始化一个空的切块列表，用于存储非空的切块。
	chunks := parts[:0]
	// 遍历切分结果，去除空白块并添加到最终的切块列表中。
	for _, part := range parts {
		// 去除切块首尾空白。
		if part = strings.TrimSpace(part); part != "" {
			// 将非空切块添加到最终的切块列表中。
			chunks = append(chunks, part)
		}
	}
	// 如果最终的切块列表为空，返回错误，表示文档未生成任何切块。
	if len(chunks) == 0 {
		return nil, errors.New("document produced no chunks")
	}
	return chunks, nil
}
