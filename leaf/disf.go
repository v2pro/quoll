package leaf

import (
	"net/http"
    "strings"
	"github.com/json-iterator/go"
)

type disfEndpoint struct {
    endpointId  int
    cuuid       string
    protocol    string
    port        int
    ip          string
    status      int
    weight      int
    updateTime  string
    createTime  string
}

type disfSearchResult struct {
    code    int
    message string
    data    []disfEndpoint
}

func endpoint2Name(endp string) string {

    var servNames string

    ret := disfSearchResult{}
    rsp, err := http.Get("http://100.70.241.15:9527/disf/endpoint/search?userName=quoll&ip=" + key)
    //rsp, err := http.Get("http://127.0.0.1:9527/disf/endpoint/search?userName=quoll&ip=" + endp)
    if err != nil {
        return ""
    }

    err = jsoniter.NewDecoder(rsp.Body).Decode(&ret)
    if err != nil {
        return ""
    }

    for _, d := range ret.data {
        s := strings.Split(d.cuuid, "@")
        if len(s) > 1 {
            servNames += "," + s[1]
        } else {
            servNames += "," + s[0]
        }
    }

    return servNames[1:]
}
