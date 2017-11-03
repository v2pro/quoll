package main

import (
    "net/http"
    "github.com/v2pro/plz/countlog"
    "runtime"
    "github.com/v2pro/quoll/leaf"
    _ "net/http/pprof"
)

func main() {
	runtime.GOMAXPROCS(1)
	logWriter := countlog.NewAsyncLogWriter(
		countlog.LEVEL_DEBUG, countlog.NewFileLogOutput("STDERR"))
	logWriter.EventWhitelist["event!discr.SceneOf"] = true
	logWriter.Start()
	countlog.LogWriters = append(countlog.LogWriters, logWriter)
	err := leaf.RegisterHttpHandlers(http.DefaultServeMux)
	if err != nil {
		countlog.Error("event!agent.start failed", "err", err)
		return
	}
	addr := ":8005"
	countlog.Info("event!agent.start", "addr", addr)
	err = http.ListenAndServe(addr, http.DefaultServeMux)
	countlog.Info("event!agent.stop", "err", err)
}
