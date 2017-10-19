package discr

import (
	"testing"
	"github.com/stretchr/testify/require"
	"regexp"
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
	should.Equal(map[string]string{
		"product_id": "1",
		"combo_type": "3",
	}, scene.ToMap())
}

func Benchmark_small_string(b *testing.B) {
	p, err := regexp.Compile(`product_id=(\d+)`)
	if err != nil {
		b.Error(err)
		return
	}
	input := []byte(`product_id=1`)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.FindSubmatch(input)
	}
}