package discr

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_match(t *testing.T) {
	should := require.New(t)
	pg, err := newPatternGroup(map[string]string{
		"product_id": `product_id=(\d+)`,
		"combo_type": `combo_type=(\d+)`})
	should.Nil(err)
	matches, err := pg.match([]byte(`product_id=1&combo_type=3`))
	should.Nil(err)
	should.Len(matches, 2)
	scene := matches.ToScene()
	should.Equal(Scene{}.appendFeature(
		[]byte("product_id"), []byte("1")).appendFeature(
		[]byte("combo_type"), []byte("3")), scene)
}