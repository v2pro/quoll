package discr

import (
	"github.com/v2pro/gohs/hyperscan"
	"regexp"
	"github.com/v2pro/plz/countlog"
)

type patternGroup struct {
	hdb hyperscan.BlockDatabase
	exps []*regexp.Regexp
	scratch *hyperscan.Scratch
	keys [][]byte
}

type patternMatch struct {
	match []byte
	exp *regexp.Regexp
	key []byte
}

type patternMatches []patternMatch

func (pms patternMatches) ToScene() Scene {
	var scene Scene
	for _, pm := range pms {
		value := pm.exp.FindSubmatch(pm.match)[1]
		scene = scene.appendFeature(pm.key, value)
	}
	return scene
}

func newPatternGroup(patterns map[string]string) (*patternGroup, error) {
	hpatterns := make([]*hyperscan.Pattern, len(patterns))
	exps := make([]*regexp.Regexp, len(patterns))
	keys := make([][]byte, len(patterns))
	var err error
	i := 0
	for key, pattern := range patterns {
		keys[i] = []byte(key)
		hpatterns[i] = hyperscan.NewPattern(pattern, hyperscan.DotAll|hyperscan.SomLeftMost)
		hpatterns[i].Id = i
		exps[i], err = regexp.Compile(pattern)
		if err != nil {
			countlog.Error("event!failed to compile pattern as regexp", "err", err, "pattern", pattern)
			return nil, err
		}
		i++
	}
	hdb, err := hyperscan.NewBlockDatabase(hpatterns...)
	if err != nil {
		countlog.Error("event!failed to compile patterns as hyperscan block database", "err", err)
		return nil, err
	}
	scratch, err := hyperscan.NewScratch(hdb)
	if err != nil {
		countlog.Error("event!failed to create scratch", "err", err)
		return nil, err
	}
	return &patternGroup{
		hdb: hdb,
		exps: exps,
		scratch: scratch,
		keys: keys,
	}, nil
}

func (pg *patternGroup) match(bytes []byte) (patternMatches, error) {
	var matches patternMatches
	err := pg.hdb.Scan(bytes, pg.scratch, func(id uint, from, to uint64, flags uint, context interface{}) error{
		matches = append(matches, patternMatch{
			match: bytes[from:to],
			exp: pg.exps[id],
			key: pg.keys[id],
		})
		return nil
	}, nil)
	if err != nil {
		return nil, err
	}
	return matches, nil
}