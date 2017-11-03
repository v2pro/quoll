package main

import (
	"io/ioutil"
	"github.com/json-iterator/go"
    "fmt"
    "strconv"
)

type eventRecordPeer struct {
    Zone    string  `json:"Zone"`
    IP      string  `json:"IP"`
    Port    int     `json:"Port"`
}

type eventRecordAction struct {
    ActionType      string  `json:"ActionType"`
    Request         string  `json:"Request"`
    Response        string  `json:"Response"`
    ResponseTime    int64   `json:"ResponseTime"`
    Peer            eventRecordPeer
    SocketFd        int
    OccurredAt      int64
    Content         string
    ActionIndex     int
}

type eventRecordCallFromInbound eventRecordAction
type eventRecordReturnInbound eventRecordAction

type eventRecord struct {
    Context             string                      `json:"Context"`
    SessionId           string
    sinkTime            int64
    Actions             []eventRecordAction         `json:"Actions"`
    CallFromInbound     eventRecordCallFromInbound 
    ReturnInbound       eventRecordReturnInbound 
}


func main() {

    record := eventRecord{}

    content, err := ioutil.ReadFile("/tmp/dump")
    err = jsoniter.Unmarshal(content, &record)
	if err != nil {
		return
	}

    var servNames map[string]string
    servNames = make(map[string]string)

    for _, act := range record.Actions {
        if act.Peer.IP == "" || act.Peer.Port == 9891 {
            continue
        }

        key := act.Peer.IP + strconv.Itoa(act.Peer.Port)

        _, ok := servNames[key]
        if ok {
            continue
        }

        // http://100.70.241.15:9527/disf/endpoint/search?userName=quoll&ip=127.0.0.1:8080
        servNames[key] = "disf!test"

        fmt.Printf("%+v\n", act.Peer)
    }
}
