package discr

import (
	"testing"
	"regexp"
	"github.com/stretchr/testify/require"
	"fmt"
	"io/ioutil"
	"github.com/json-iterator/go"
	"github.com/flier/gohs/hyperscan"
	"runtime/debug"
)

func Test_regexp(t *testing.T) {
	should := require.New(t)
	content, err := ioutil.ReadFile("/home/xiaoju/sample.txt")
	should.Nil(err)
	text := jsoniter.Get(content, "Request").ToString()
	re, err := regexp.Compile(`(?:"product_id":\s*(?P<product_id>\d+))|(?:"combo_type":\s*(?P<combo_type>\d+))`)
	//re, err := regexp.Compile(`"user_name":\s*"(\w+)"`)
	should.Nil(err)
	subMatches := re.FindAllSubmatch([]byte(text), -1)[1]
	fmt.Println(string(subMatches[1]))
}

func Benchmark_regexp(b *testing.B) {
	content, err := ioutil.ReadFile("/home/xiaoju/sample.txt")
	if err != nil {
		b.Error(err)
		return
	}
	text := []byte(jsoniter.Get(content, "Request").ToString())
	productId, err := regexp.Compile(`"product_id":\s*\d+`)
	comboType, err := regexp.Compile(`"combo_type":\s*\d+`)
	carLevel, err := regexp.Compile(`"car_level":\s*"\d+"`)
	if err != nil {
		b.Error(err)
		return
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		productId.FindAll(text, -1)
		comboType.FindAll(text, -1)
		carLevel.FindAll(text, -1)
	}
}

func Benchmark_regexp_batch(b *testing.B) {
	content, err := ioutil.ReadFile("/home/xiaoju/sample.txt")
	if err != nil {
		b.Error(err)
		return
	}
	text := []byte(jsoniter.Get(content, "Request").ToString())
	p1 := `(?:"product_id":\s*(?P<product_id>\d+))`
	p2 := `(?:"combo_type":\s*(?P<combo_type>\d+))`
	p3 := `(?:"car_level":\s*"(?P<car_level>\d+)")`
	re, err := regexp.Compile(p1+"|"+p2+"|"+p3)
	if err != nil {
		b.Error(err)
		return
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		re.FindAllSubmatch(text,-1)
	}
}


func Benchmark_hyperscan(b *testing.B) {
	content, err := ioutil.ReadFile("/home/xiaoju/sample.txt")
	if err != nil {
		b.Error(err)
		return
	}
	text := jsoniter.Get(content, "Request").ToString()
	p1 := hyperscan.NewPattern(`"product_id":\s*(\d+)`, hyperscan.DotAll|hyperscan.SomLeftMost)
	p2 := hyperscan.NewPattern(`"combo_type":\s*(\d+)`, hyperscan.DotAll|hyperscan.SomLeftMost)
	p3 := hyperscan.NewPattern(`"car_level":\s*"(\d+)"`, hyperscan.DotAll|hyperscan.SomLeftMost)
	db, err := hyperscan.NewBlockDatabase(p1, p2, p3)
	if err != nil {
		b.Error(err)
		return
	}
	defer db.Close()
	scratch, err := hyperscan.NewScratch(db)
	if err != nil {
		b.Error(err)
		return
	}
	defer scratch.Free()
	textAsBytes := []byte(text)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		db.FindAll(textAsBytes, -1)
	}
}

func Test_hyperscan(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered != nil {
			fmt.Println(recovered)
			debug.PrintStack()
		}
	}()
	should := require.New(t)
	content, err := ioutil.ReadFile("/home/xiaoju/sample.txt")
	should.Nil(err)
	text := jsoniter.Get(content, "Request").ToString()
	p1 := hyperscan.NewPattern(`"product_id":\s*(\d+)`, hyperscan.DotAll|hyperscan.SomLeftMost)
	p2 := hyperscan.NewPattern(`"combo_type":\s*(\d+)`, hyperscan.DotAll|hyperscan.SomLeftMost)
	p3 := hyperscan.NewPattern(`"car_level":\s*"(\d+)"`, hyperscan.DotAll|hyperscan.SomLeftMost)
	db, err := hyperscan.NewBlockDatabase(p1, p2, p3)
	should.Nil(err)
	defer db.Close()
	scratch, err := hyperscan.NewScratch(db)
	should.Nil(err)
	defer scratch.Free()
	all := db.FindAllString(text, -1)
	info, err := db.Info()
	should.Nil(err)
	fmt.Println(info)
	for _, p := range all {
		fmt.Println(p)
	}
}
