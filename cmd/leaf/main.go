package main

import (
	"net/http"
	"github.com/v2pro/plz/countlog"
	"runtime"
	"github.com/v2pro/quoll/leaf"
)

func main() {
	runtime.GOMAXPROCS(1)
	mux, err := leaf.NewServeMux()
	if err != nil {
		countlog.Error("event!agent.start failed", "err", err)
		return
	}
	addr := "127.0.0.1:1026"
	countlog.Info("event!agent.start", "addr", addr)
	err = http.ListenAndServe(addr, mux)
	countlog.Info("event!agent.stop", "err", err)
}