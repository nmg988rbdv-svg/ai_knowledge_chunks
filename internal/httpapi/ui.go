package httpapi

import (
	_ "embed"
	"net/http"
)

//go:embed ui/index.html
var indexHTML []byte

// ui 路由，提供前端界面。
func (h *Handler) ui(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}
