package main

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/quoll/evtstore"
	"github.com/v2pro/quoll/discr"
	"github.com/json-iterator/go"
	"runtime"
	"time"
	"strconv"
)

var store = evtstore.NewStore("/tmp/store")

func main() {
	runtime.GOMAXPROCS(1)
	err := store.Start()
	if err != nil {
		return
	}
	http.HandleFunc("/add-event", AddEvent)
	http.HandleFunc("/list-events", ListEvents)
	http.HandleFunc("/update-session-matcher", UpdateSessionMatcher)
	addr := "127.0.0.1:1026"
	countlog.Info("event!agent.start", "addr", addr)
	err = http.ListenAndServe(addr, nil)
	countlog.Info("event!agent.stop", "err", err)
}

func AddEvent(respWriter http.ResponseWriter, req *http.Request) {
	eventJson, err := ioutil.ReadAll(req.Body)
	if err != nil {
		writeError(respWriter, err)
		return
	}
	err = store.Add(eventJson)
	if err != nil {
		writeError(respWriter, err)
		return
	}
	respWriter.Write([]byte(`{"errno":0}`))
}

func ListEvents(respWriter http.ResponseWriter, req *http.Request) {
	startTime := time.Now().Add(-time.Hour)
	query := req.URL.Query()
	startTimeStr := query.Get("startTime")
	var err error
	if startTimeStr != "" {
		startTime, err = time.ParseInLocation("201701010800", startTimeStr, evtstore.CST)
		if err != nil {
			writeError(respWriter, err)
			return
		}
	}
	endTime := time.Now()
	endTimeStr := query.Get("endTime")
	if endTimeStr != "" {
		endTime, err = time.ParseInLocation("201701010800", endTimeStr, evtstore.CST)
		if err != nil {
			writeError(respWriter, err)
			return
		}
	}
	skip := 0
	skipStr := query.Get("skip")
	if skipStr != "" {
		skip, err = strconv.Atoi(skipStr)
		if err != nil {
			writeError(respWriter, err)
			return
		}
	}
	limit := 10
	limitStr := query.Get("limit")
	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			writeError(respWriter, err)
			return
		}
	}
	blocks, err := store.List(startTime, endTime, skip, limit)
	if err != nil {
		writeError(respWriter, err)
		return
	}
	_, err = respWriter.Write(blocks)
	if err != nil {
		countlog.Error("event!failed to write blocks", "err", err)
	}
}

func UpdateSessionMatcher(respWriter http.ResponseWriter, req *http.Request) {
	var cnf discr.SessionMatcherCnf
	decoder := jsoniter.NewDecoder(req.Body)
	defer req.Body.Close()
	err := decoder.Decode(&cnf)
	if err != nil {
		writeError(respWriter, err)
		return
	}
	err = discr.UpdateSessionMatcher(cnf)
	if err != nil {
		writeError(respWriter, err)
		return
	}
	respWriter.Write([]byte(`{"errno":0}`))
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
