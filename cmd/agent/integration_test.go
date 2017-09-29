package main

import (
	"testing"
	"net/http"
	"strings"
	"github.com/stretchr/testify/require"
	"io/ioutil"
)

func Test(t *testing.T) {
	should := require.New(t)
	resp, err := http.Post("http://127.0.0.1:1026/add-event", "application/json", strings.NewReader(`
	{"url": "/"}
	`))
	should.Nil(err)
	respBody, err := ioutil.ReadAll(resp.Body)
	should.Nil(err)
	should.Equal(`{"errno":0}`, string(respBody))
}