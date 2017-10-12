package main

import (
	"testing"
	"net/http"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"bytes"
)

func Test(t *testing.T) {
	should := require.New(t)
	content, err := ioutil.ReadFile("/home/xiaoju/sample.txt")
	should.Nil(err)
	resp, err := http.Post("http://127.0.0.1:1026/add-event", "application/json", bytes.NewBuffer(content))
	should.Nil(err)
	respBody, err := ioutil.ReadAll(resp.Body)
	should.Nil(err)
	should.Equal(`{"errno":0}`, string(respBody))
}