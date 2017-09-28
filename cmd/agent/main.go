package main

import (
	"net/http"
	"github.com/v2pro/quoll/event"
	"io/ioutil"
	"encoding/json"
	"github.com/v2pro/koala/countlog"
)

func main() {
	http.HandleFunc("/add-event", func(respWriter http.ResponseWriter, req *http.Request) {
		eventJson, err := ioutil.ReadAll(req.Body)
		if err != nil {
			writeError(respWriter, err)
			return
		}
		err = event.Add(req.URL.Query().Get("event_type"), eventJson)
		if err != nil {
			writeError(respWriter, err)
			return
		}
		respWriter.Write([]byte(`{"errno":0}`))
	})
	addr := "127.0.0.1:1026"
	countlog.Info("event!agent.start", "addr", addr)
	err := http.ListenAndServe(addr, nil)
	countlog.Info("event!agent.stop", "err", err)
}

func writeError(respWriter http.ResponseWriter, err error) {
	resp, marshalErr := json.Marshal(map[string]interface{}{
		"errno":  1,
		"errmsg": err.Error(),
	})
	if marshalErr != nil {
		countlog.Error("event!agent.failed to marshal json", "err", marshalErr)
		return
	}
	_, writeErr := respWriter.Write(resp)
	if writeErr != nil {
		countlog.Error("event!agent.failed to write response", "err", writeErr)
		return
	}
}
