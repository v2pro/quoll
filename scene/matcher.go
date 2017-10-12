package scene

import (
	"github.com/v2pro/quoll/evtstore"
	"github.com/v2pro/gohs/hyperscan"
	"github.com/json-iterator/go"
	"github.com/v2pro/plz/countlog"
	"bytes"
	"errors"
)

var sessionMatchers = map[string]*sessionMatcher{}

type sessionMatcher struct {
	sessionType string // url
	callOutbounds map[string]*callOutboundMatcher
	inboundRequestHbd hyperscan.BlockDatabase
	inboundResponseHbd hyperscan.BlockDatabase
}

type callOutboundMatcher struct {
	serviceName string
	requestHbd hyperscan.BlockDatabase
	responseHbd hyperscan.BlockDatabase
}

type SessionMatcherCnf struct {
	SessionType string
	InboundRequestPatterns []string
	InboundResponsePatterns []string
	CallOutbounds []CallOutboundMatcherCnf
}

type CallOutboundMatcherCnf struct {
	ServiceName string
	RequestPatterns []string
	ResponsePatterns []string
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
			requestHbd: requestHbd,
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
		sessionType: cnf.SessionType,
		callOutbounds: callOutbounds,
		inboundRequestHbd: inboundRequestHbd,
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

func DiscriminateFeature(eventBody evtstore.EventBody) evtstore.EventBody {
	iter := jsoniter.ConfigFastest.BorrowIterator(eventBody)
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	collector := &featureCollector{}
	collector.colSession(iter)
	mangled := bytes.NewBuffer(make([]byte, 0, len(eventBody)))
	if err := mangled.WriteByte('['); err != nil {
		countlog.Error("event!failed to write", "err", err)
		return nil
	}
	stream := jsoniter.ConfigFastest.BorrowStream(nil)
	defer jsoniter.ConfigFastest.ReturnStream(stream)
	stream.WriteArrayStart()
	stream.WriteVal(collector.feature)
	stream.WriteMore()
	stream.Write(eventBody)
	stream.WriteArrayEnd()
	return stream.Buffer()
}

var sessionTypeStart = []byte(`\\x0bQREQUEST_URI`)
var sessionTypeEnd = []byte(`\\x`)

type featureCollector struct {
	feature []string
}

func (collector *featureCollector) colSession(iter *jsoniter.Iterator) {
	var sessionMatcher *sessionMatcher
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "CallFromInbound":
			sessionMatcher = collector.colCallFromInbound(iter)
		case "ReturnInbound":
			collector.colReturnInbound(iter, sessionMatcher.inboundResponseHbd)
		default:
			iter.Skip()
		}
		return true
	})
}

func (collector *featureCollector) colCallFromInbound(iter *jsoniter.Iterator) (sessionMatcher *sessionMatcher) {
	iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "Request":
			req := iter.ReadStringAsSlice()
			startPos := bytes.Index(req, sessionTypeStart)
			if startPos == -1 {
				iter.Error = errors.New("session type start can not be found")
				return true
			}
			partialReq := req[startPos + len(sessionTypeStart):]
			endPos := bytes.Index(partialReq, sessionTypeEnd)
			if endPos == -1 {
				iter.Error = errors.New("session type end can not be found")
				return true
			}
			sessionType := string(partialReq[:endPos])
			sessionMatcher = sessionMatchers[sessionType]
			if sessionMatcher != nil && sessionMatcher.inboundRequestHbd != nil {
				feature := sessionMatcher.inboundRequestHbd.FindAll(req, -1)
				for _, featureItem := range feature {
					collector.feature = append(collector.feature, string(featureItem))
				}
			}
		default:
			iter.Skip()
		}
		return true
	})
	return
}

func (collector *featureCollector) colReturnInbound(iter *jsoniter.Iterator, inboundResponseHbd hyperscan.BlockDatabase) {
	iter.ReadObjectCB(func (iter *jsoniter.Iterator, field string) bool {
		switch field {
		case "Response":
			resp := iter.ReadStringAsSlice()
			if inboundResponseHbd != nil {
				feature := inboundResponseHbd.FindAll(resp, -1)
				for _, featureItem := range feature {
					collector.feature = append(collector.feature, string(featureItem))
				}
			}
		default:
			iter.Skip()
		}
		return true
	})
}