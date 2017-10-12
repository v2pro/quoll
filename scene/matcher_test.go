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
	mangled := DiscriminateFeature([]byte(session))
	should.Equal(`[["product_id=3","combo_type=1"],` + session + `]`, string(mangled))
}