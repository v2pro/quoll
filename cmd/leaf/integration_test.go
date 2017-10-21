package main

import (
	"testing"
	"net/http"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"bytes"
	"time"
	"fmt"
	"path"
	"github.com/v2pro/quoll/evtstore"
)

func Test_list(t *testing.T) {
	should := require.New(t)
	resp, err := http.Get("http://127.0.0.1:1026/list-events")
	should.Nil(err)
	body, err := ioutil.ReadAll(resp.Body)
	should.Nil(err)
	blocks := evtstore.EventBlocks(body)
	blockId, _, _ := blocks.Next()
	should.Len(blockId.FileName(), 12)
}

func Test_add(t *testing.T) {
	should := require.New(t)
	resp, err := http.Post("http://127.0.0.1:1026/update-session-matcher",
		"application/json", bytes.NewBufferString(`
		{
			"SessionType": "/application/passenger/v2/index.php/core/pNewOrder",
			"KeepNSessionsPerScene": 1,
			"CallOutbounds": [
				{
					"ServiceName": "Carrera",
					"RequestPatterns": {"product_id": "\"product_id\":(\\d+)"}
				}
			]
		}
	`))
	should.Nil(err)
	respBody, err := ioutil.ReadAll(resp.Body)
	should.Nil(err)
	should.Equal(`{"errno":0}`, string(respBody))
	files, err := ioutil.ReadDir("/home/xiaoju/testdata2")
	should.Nil(err)
	contents := [][]byte{}
	totalSize := 0
	for _, file := range files[:12] {
		content, err := ioutil.ReadFile(path.Join("/home/xiaoju/testdata2", file.Name()))
		should.Nil(err)
		contents = append(contents, content)
		totalSize += len(content)
	}
	before := time.Now()
	for _, content := range contents {
		resp, err := http.Post("http://127.0.0.1:1026/add-event", "application/json", bytes.NewBuffer(content))
		should.Nil(err)
		respBody, err := ioutil.ReadAll(resp.Body)
		should.Nil(err)
		should.Equal(`{"errno":0}`, string(respBody))
	}
	after := time.Now()
	fmt.Println(totalSize)
	fmt.Println(len(contents))
	fmt.Println(after.Sub(before))
}