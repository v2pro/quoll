package scene

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_end_to_end(t *testing.T) {
	should := require.New(t)
	err := UpdateSessionMatcher(SessionMatcherCnf{
		SessionType: "/test",
		InboundRequestPatterns: []string{"xxx"},
		InboundResponsePatterns: []string{`product_id=\d+`,`combo_type=\d+`},
		CallOutbounds: []CallOutboundMatcherCnf{
			{
				ServiceName: "passport",
				RequestPatterns: []string{`"user_role":\s*"\w+"`},
				ResponsePatterns: []string{`"user_type":\s*"\w+"`},
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
	mangled := DiscriminateFeature([]byte(session))
	should.Equal(`[["product_id=3","combo_type=1","\"user_role\":\"driver\"","\"user_type\":\"normal\""],` +
		session + `]`, string(mangled))
}