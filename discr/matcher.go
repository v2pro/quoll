package discr

import (
	"github.com/v2pro/quoll/evtstore"
	"github.com/v2pro/gohs/hyperscan"
	"github.com/json-iterator/go"
	"bytes"
	"errors"
	"github.com/v2pro/plz/countlog"
	"math"
	"encoding/binary"
)

var sessionMatchers = map[string]*sessionMatcher{}

type sessionMatcher struct {
	sessionType        string // url
	callOutbounds      map[string]*callOutboundMatcher
	inboundRequestHbd  hyperscan.BlockDatabase
	inboundResponseHbd hyperscan.BlockDatabase
}

type callOutboundMatcher struct {
	serviceName string
	requestHbd  hyperscan.BlockDatabase
	responseHbd hyperscan.BlockDatabase
}

type SessionMatcherCnf struct {
	SessionType             string
	InboundRequestPatterns  []string
	InboundResponsePatterns []string
	CallOutbounds           []CallOutboundMatcherCnf
}

type CallOutboundMatcherCnf struct {
	ServiceName      string
	RequestPatterns  []string
	ResponsePatterns []string
}

type Feature []byte
type Scene []byte

func (s Scene) appendFeature(key, value []byte) Scene {
	if len(key) > math.MaxUint16 {
		key = key[:math.MaxUint16]
	}
	if len(value) > math.MaxUint16 {
		value = value[:math.MaxUint16]
	}
	s = append(s, []byte{0, 0}...)
	binary.LittleEndian.PutUint16(s[len(s)-2:], uint16(len(key)))
	s = append(s, key...)
	s = append(s, []byte{0, 0}...)
	binary.LittleEndian.PutUint16(s[len(s)-2:], uint16(len(key)))
	s = append(s, value...)
	return s
}

func UpdateSessionMatcher(cnf SessionMatcherCnf) error {
	callOutbounds := map[string]*callOutboundMatcher{}
	for _, callOutbound := range cnf.CallOutbounds {
		requestHbd, err := createHbd(callOutbound.RequestPatterns)
		if err != nil {
			return err
		}
		responseHbd, err := createHbd(callOutbound.ResponsePatterns)
		if err != nil {
			return err
		}
		callOutbounds[callOutbound.ServiceName] = &callOutboundMatcher{
			serviceName: callOutbound.ServiceName,
			requestHbd:  requestHbd,
			responseHbd: responseHbd,
		}
	}
	inboundRequestHbd, err := createHbd(cnf.InboundRequestPatterns)
	if err != nil {
		return err
	}
	inboundResponseHbd, err := createHbd(cnf.InboundResponsePatterns)
	if err != nil {
		return err
	}
	sessionMatcher := &sessionMatcher{
		sessionType:        cnf.SessionType,
		callOutbounds:      callOutbounds,
		inboundRequestHbd:  inboundRequestHbd,
		inboundResponseHbd: inboundResponseHbd,
	}
	sessionMatchers[cnf.SessionType] = sessionMatcher
	return nil
}

func createHbd(patterns []string) (hyperscan.BlockDatabase, error) {
	if len(patterns) == 0 {
		return nil, nil
	}
	compiledPatterns := []*hyperscan.Pattern{}
	for _, pattern := range patterns {
		compiledPatterns = append(compiledPatterns, hyperscan.NewPattern(
			pattern, hyperscan.DotAll|hyperscan.SomLeftMost))
	}
	return hyperscan.NewBlockDatabase(compiledPatterns...)
}

func DiscriminateFeature(eventBody evtstore.EventBody) Feature {
	iter := jsoniter.ConfigFastest.BorrowIterator(eventBody)
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	collector := &featureCollector{iter: iter}
	collector.colSession()
	if iter.Error != nil {
		countlog.Error("event!failed to parse session", "err", iter.Error)
		return nil
	}
	return nil
}

var sessionTypeStart = []byte(`\x0bQREQUEST_URI`)
var sessionTypeEnd = []byte(`\x`)

type featureCollector struct {
	iter           *jsoniter.Iterator
	feature        []string
	sessionMatcher *sessionMatcher
}

func (collector *featureCollector) colSession() {
	collector.iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "CallFromInbound":
			collector.sessionMatcher = collector.colCallFromInbound()
		case "ReturnInbound":
			collector.colReturnInbound()
		case "Actions":
			collector.colActions()
		default:
			iter.Skip()
		}
		return true
	})
}

func (collector *featureCollector) colCallFromInbound() (sessionMatcher *sessionMatcher) {
	collector.iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "Request":
			req := []byte(iter.ReadString())
			startPos := bytes.Index(req, sessionTypeStart)
			if startPos == -1 {
				iter.Error = errors.New("session type start can not be found")
				return true
			}
			partialReq := req[startPos+len(sessionTypeStart):]
			endPos := bytes.Index(partialReq, sessionTypeEnd)
			if endPos == -1 {
				iter.Error = errors.New("session type end can not be found")
				return true
			}
			sessionType := string(partialReq[:endPos])
			sessionMatcher = sessionMatchers[sessionType]
			if sessionMatcher != nil {
				collector.matchFeature(req, sessionMatcher.inboundRequestHbd)
			}
		default:
			iter.Skip()
		}
		return true
	})
	return
}

func (collector *featureCollector) colReturnInbound() {
	collector.iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "Response":
			resp := []byte(iter.ReadString())
			if collector.sessionMatcher != nil {
				collector.matchFeature(resp, collector.sessionMatcher.inboundResponseHbd)
			}
		default:
			iter.Skip()
		}
		return true
	})
}

func (collector *featureCollector) colActions() {
	collector.iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
		var callOutboundMatcher *callOutboundMatcher
		iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
			switch field {
			case "ServiceName":
				serviceName := iter.ReadString()
				if collector.sessionMatcher != nil {
					callOutboundMatcher = collector.sessionMatcher.callOutbounds[serviceName]
				}
			case "Request":
				req := []byte(iter.ReadString())
				if callOutboundMatcher != nil {
					collector.matchFeature(req, callOutboundMatcher.requestHbd)
				}
			case "Response":
				resp := []byte(iter.ReadString())
				if callOutboundMatcher != nil {
					collector.matchFeature(resp, callOutboundMatcher.responseHbd)
				}
			default:
				iter.Skip()
			}
			return true
		})
		return true
	})
}

func (collector *featureCollector) matchFeature(bytes []byte, hbd hyperscan.BlockDatabase) {
	if hbd == nil {
		return
	}
	feature := hbd.FindAll(bytes, -1)
	for _, featureItem := range feature {
		collector.feature = append(collector.feature, string(featureItem))
	}
}
