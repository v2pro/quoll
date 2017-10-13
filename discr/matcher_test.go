package discr

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_end_to_end(t *testing.T) {
	should := require.New(t)
	err := UpdateSessionMatcher(SessionMatcherCnf{
		SessionType:            "/test",
		KeepNSessionsPerScene: 1,
		InboundRequestPatterns: map[string]string{"xxx": "xxx"},
		InboundResponsePatterns: map[string]string{
			"product_id": `product_id=(\d+)`,
			"combo_type": `combo_type=(\d+)`,
		},
		CallOutbounds: []CallOutboundMatcherCnf{
			{
				ServiceName: "passport",
				RequestPatterns: map[string]string{
					"user_role": `"user_role":\s*"(\w+)"`,
				},
				ResponsePatterns: map[string]string{
					"user_type": `"user_type":\s*"(\w+)"`,
				},
			},
		},
	})
	should.Nil(err)
	session := `{
	"CallFromInbound": {
		"Request": "\\x0bQREQUEST_URI/test\\x0c2DOCUMENT_URI"
	},
	"ReturnInbound": {
		"Response": "product_id=3&combo_type=1"
	},
	"Actions": [
		{
			"ActionType": "CallOutbound",
			"ServiceName": "passport",
			"Request": "{\"user_role\":\"driver\"}",
			"Response": "{\"user_type\":\"normal\"}"
		}
	]
}`
	ds := DeduplicationState{}
	scene := ds.SceneOf([]byte(session))
	should.Equal(Scene{}.appendFeature(
		[]byte("product_id"), []byte("3")).appendFeature(
		[]byte("combo_type"), []byte("1")).appendFeature(
		[]byte("user_role"), []byte("driver")).appendFeature(
		[]byte("user_type"), []byte("normal")), scene)
}

func Test_keep_n(t *testing.T) {
	should := require.New(t)
	err := UpdateSessionMatcher(SessionMatcherCnf{
		SessionType:            "/test",
		KeepNSessionsPerScene: 2,
		InboundRequestPatterns: map[string]string{"xxx": "xxx"},
		InboundResponsePatterns: map[string]string{
			"product_id": `product_id=(\d+)`,
		},
	})
	should.Nil(err)
	session := `{
	"CallFromInbound": {
		"Request": "\\x0bQREQUEST_URI/test\\x0c2DOCUMENT_URI"
	},
	"ReturnInbound": {
		"Response": "product_id=3&combo_type=1"
	}
}`
	ds := DeduplicationState{}
	should.Equal(Scene{}.appendFeature(
		[]byte("product_id"), []byte("3")), ds.SceneOf([]byte(session)))
	should.Equal(Scene{}.appendFeature(
		[]byte("product_id"), []byte("3")), ds.SceneOf([]byte(session)))
	should.Equal(Scene(nil), ds.SceneOf([]byte(session)))
}
