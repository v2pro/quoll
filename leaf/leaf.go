package leaf

import (
	"net/http"
	"io/ioutil"
	"encoding/json"
	"github.com/v2pro/plz/countlog"
	"github.com/v2pro/quoll/evtstore"
	"github.com/v2pro/quoll/discr"
	"github.com/json-iterator/go"
	"time"
	"strconv"
)

type eventRecordPeer struct {
    Zone    string  `json:"Zone"`
    IP      string  `json:"IP"`
    Port    int     `json:"Port"`
}

type eventRecordAction struct {
    // CallFromInbound, ReturnInbound also use this struct
    // some fields will be omitted if not exists
    ActionType      string  `json:"ActionType"`
    ServiceName     string  `json:"ServiceName,omitempty"`
    Request         string  `json:"Request,omitempty"`  // not in ReturnInbound
    Response        string  `json:"Response,omitempty"` // not in CallFromInbound
    ResponseTime    int64   `json:"ResponseTime,omitempty"`
    Peer            eventRecordPeer `json:"Peer"`
    SocketFd        int     `json:"SocketFD,omitempty"` // not in ReturnInbound, CallFromInbound
    OccurredAt      int64   `json:"OccurredAt"`
    Content         string  `json:"Content,omitempty"`
    ActionIndex     int     `json:"ActionIndex"`
}

type eventRecordCallFromInbound eventRecordAction
type eventRecordReturnInbound eventRecordAction

type eventRecord struct {
    Context             string              `json:"Context"`
    SessionId           string              `json:"SessionId"`
    SinkTime            int64               `json:"sinkTime"`
    Actions             []eventRecordAction `json:"Actions"`
    CallFromInbound     eventRecordCallFromInbound  `json:"CallFromInbound"`
    ReturnInbound       eventRecordReturnInbound    `json:"ReturnInbound"`
}

var store = evtstore.NewStore("/tmp/store")

func RegisterHttpHandlers(mux *http.ServeMux) error {
	err := store.Start()
	if err != nil {
		return err
	}
	mux.HandleFunc("/add-event", addEvent)
	mux.HandleFunc("/list-events", listEvents)
	mux.HandleFunc("/update-session-matcher", updateSessionMatcher)
	mux.HandleFunc("/tail", tail)
	mux.HandleFunc("/", showTailForm)
	return nil
}

func addEvent(respWriter http.ResponseWriter, req *http.Request) {
	eventJson, err := ioutil.ReadAll(req.Body)
	if err != nil {
		writeError(respWriter, err)
		return
	}
    
    // get disf service name from ip:port
    record := eventRecord{}
    err = jsoniter.Unmarshal(eventJson, &record)

    if err != nil {
        writeError(respWriter, err)
        return
    }

    var servNames map[string]string
    servNames = make(map[string]string)
    for i, act := range record.Actions {
        if act.Peer.IP == "" || act.Peer.Port == 9891 {
            continue
        }

        key := act.Peer.IP + strconv.Itoa(act.Peer.Port)

        _, ok := servNames[key]
        if ok {
            record.Actions[i].ServiceName = servNames[key]
        } else {
            servNames[key] = endpoint2Name(key)
            record.Actions[i].ServiceName = servNames[key]
        }
    }

    eventJson, err = jsoniter.Marshal(record)
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

func listEvents(respWriter http.ResponseWriter, req *http.Request) {
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

func updateSessionMatcher(respWriter http.ResponseWriter, req *http.Request) {
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

func tail(respWriter http.ResponseWriter, req *http.Request) {
	respWriter.Write([]byte("<html><body>"))
	err := req.ParseForm()
	if err != nil {
		respWriter.Write([]byte(err.Error()))
		return
	}
	sessionType := req.Form.Get("sessionType")
	respWriter.Write([]byte("sessionType: " + sessionType + "<br/>"))
	showSession := req.Form.Get("showSession")
	respWriter.Write([]byte("showSession: " + showSession + "<br/>"))
	limitStr := req.Form.Get("limit")
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		respWriter.Write([]byte(err.Error()))
		return
	}
	respWriter.Write([]byte("limit: " + limitStr + "<br/>"))
	matcher := req.Form.Get("matcher")
	matcherCnf := discr.SessionMatcherCnf{}
	err = jsoniter.Unmarshal([]byte(matcher), &matcherCnf)
	if err != nil {
		respWriter.Write([]byte(err.Error()))
		return
	}
	respWriter.Write([]byte("matcher: <pre>" + matcher + "</pre><br/>"))
	if f, ok := respWriter.(http.Flusher); ok {
		f.Flush()
	}
	discr.Tail(respWriter, sessionType, showSession == "on", limit, matcherCnf)
}

func showTailForm(respWriter http.ResponseWriter, req *http.Request) {
	respWriter.Write([]byte(`
<html>
<body>
	<form action="/tail" method="POST" target="_blank">
			Session Type: <input type="textbox" name="sessionType" style="width: 40em;"/><br/>
			Show Session: <input type="checkbox" name="showSession"/><br/>
			Limit: <input type="number" name="limit" value="10"/><br/>
			Matcher:
<textarea rows="20" cols="60" name="matcher">
{
	"InboundRequestPatterns": {},
	"InboundResponsePatterns": {},
	"CallOutbounds": [
		{
		"RequestPatterns": {},
		"ResponsePatterns": {}
		}
	]
}
</textarea><br/>
		<button>tail</button>
	</form>
</body>
	`))
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

