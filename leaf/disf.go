package leaf

import (
    "net/http"
    "io/ioutil"
    "strings"
    "github.com/json-iterator/go"
)

type disfEndpoint struct {
    EndpointId  int     `json:"endpointId"`
    Cuuid       string  `json:"cuuid"`
    Protocol    string  `json:"protocol"`
    Port        int     `json:"port"`
    Ip          string  `json:"ip"`
    Status      int     `json:"status"`
    Weight      int     `json:"weight"`
    UpdateTime  string  `json:"updateTime"`
    CreateTime  string  `json:"createTime"`
}

type disfSearchResult struct {
    Code    int             `json:"code"`
    Message string          `json:"message"`
    Data    []disfEndpoint  `json:"data"`
}

func endpoint2Name(endp string) string {

    var servNames string

    ret := disfSearchResult{}
    rsp, err := http.Get("http://100.70.241.15:9527/disf/endpoint/search?userName=quoll&ip=" + endp)
    //rsp, err := http.Get("http://127.0.0.1:9527/disf/endpoint/search?userName=quoll&ip=" + endp)
    if err != nil {
        return ""
    }

    body, err := ioutil.ReadAll(rsp.Body)
    if err != nil {
        return ""
    }

    err = jsoniter.Unmarshal(body, &ret)
    if err != nil {
        return ""
    }

    for _, d := range ret.Data {
        s := strings.Split(d.Cuuid, "@")
        if len(s) > 1 {
            servNames += "," + s[1]
        } else {
            servNames += "," + s[0]
        }
    }

    if len(servNames) > 0 {
        return servNames[1:]
    } else {
        return ""
    }
}
