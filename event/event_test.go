package event

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_add(t *testing.T) {
	should := require.New(t)
	err := Add("access", []byte(`{"url":"/hello"}`))
	should.Nil(err)
}
