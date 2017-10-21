package discr

import (
	"github.com/json-iterator/go"
	"bytes"
	"errors"
	"github.com/v2pro/plz/countlog"
	"sync"
)

type EventBody []byte

type SessionTailer func(string, []byte)

var sessionTailers = map[string]chan SessionTailer{}
var sessionTailersMutex = &sync.Mutex{}

var sessionMatchers = map[string]*sessionMatcher{}
var sessionMatchersMutex = &sync.Mutex{}

func AddSessionTailer(sessionType string, sessionTailer SessionTailer) error {
	{
		sessionTailersMutex.Lock()
		defer sessionTailersMutex.Unlock()
		c := sessionTailers[sessionType]
		if c == nil {
			sessionTailers[sessionType] = make(chan SessionTailer, 1024)
		}
	}
	select {
	case sessionTailers[sessionType] <- sessionTailer:
		return nil
	default:
		return errors.New("overflow")
	}
}

func notifySessionTailer(sessionType string, session []byte) {
	for _, sessionTailer := range getSessionTailer("*") {
		sessionTailer(sessionType, session)
	}
}

func getSessionTailer(sessionType string) []SessionTailer {
	var c chan SessionTailer
	{
		sessionTailersMutex.Lock()
		defer sessionTailersMutex.Unlock()
		c = sessionTailers[sessionType]
	}
	if c == nil {
		return nil
	}
	var sessionTailers []SessionTailer
	for {
		select {
		case sessionTailer := <-c:
			sessionTailers = append(sessionTailers, sessionTailer)
		default:
			return sessionTailers
		}
	}
}

type sessionMatcher struct {
	sessionType           string // url
	keepNSessionsPerScene int
	callOutbounds         map[string]*callOutboundMatcher
	inboundRequestPg      *patternGroup
	inboundResponsePg     *patternGroup
}

type callOutboundMatcher struct {
	serviceName string
	requestPg   *patternGroup
	responsePg  *patternGroup
}

type SessionMatcherCnf struct {
	SessionType             string
	KeepNSessionsPerScene   int
	InboundRequestPatterns  map[string]string
	InboundResponsePatterns map[string]string
	CallOutbounds           []CallOutboundMatcherCnf
}

type CallOutboundMatcherCnf struct {
	ServiceName      string
	RequestPatterns  map[string]string
	ResponsePatterns map[string]string
}

func getSessionMatcher(sessionType string) *sessionMatcher {
	sessionMatchersMutex.Lock()
	defer sessionMatchersMutex.Unlock()
	return sessionMatchers[sessionType]
}

func UpdateSessionMatcher(cnf SessionMatcherCnf) error {
	if cnf.SessionType == "" {
		return errors.New("session type is empty")
	}
	callOutbounds := map[string]*callOutboundMatcher{}
	for _, callOutbound := range cnf.CallOutbounds {
		requestPg, err := newPatternGroup(callOutbound.RequestPatterns)
		if err != nil {
			return err
		}
		responsePg, err := newPatternGroup(callOutbound.ResponsePatterns)
		if err != nil {
			return err
		}
		callOutbounds[callOutbound.ServiceName] = &callOutboundMatcher{
			serviceName: callOutbound.ServiceName,
			requestPg:   requestPg,
			responsePg:  responsePg,
		}
	}
	inboundRequestPg, err := newPatternGroup(cnf.InboundRequestPatterns)
	if err != nil {
		return err
	}
	inboundResponsePg, err := newPatternGroup(cnf.InboundResponsePatterns)
	if err != nil {
		return err
	}
	sessionMatchersMutex.Lock()
	defer sessionMatchersMutex.Unlock()
	sessionMatcher := &sessionMatcher{
		sessionType:           cnf.SessionType,
		keepNSessionsPerScene: cnf.KeepNSessionsPerScene,
		callOutbounds:         callOutbounds,
		inboundRequestPg:      inboundRequestPg,
		inboundResponsePg:     inboundResponsePg,
	}
	sessionMatchers[cnf.SessionType] = sessionMatcher
	return nil
}

type Discrminator interface {
	SceneOf(eventBody EventBody) Scene
}

var NewDiscrminator = func() Discrminator {
	return &deduplicationState{}
}

type deduplicationState struct {
	sessionTypes map[string]sessionTypeDS
}

type sessionTypeDS map[string]int

func (ds *deduplicationState) SceneOf(eventBody EventBody) Scene {
	iter := jsoniter.ConfigFastest.BorrowIterator(eventBody)
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	collector := &featureCollector{iter: iter, session: eventBody}
	collector.colSession()
	if iter.Error != nil {
		countlog.Error("event!failed to parse session", "err", iter.Error)
		return nil
	}
	if collector.sessionMatcher == nil {
		countlog.Debug("event!filtered_because_session_type_unknown")
		return nil
	}
	if ds.sessionTypes == nil {
		ds.sessionTypes = map[string]sessionTypeDS{}
	}
	perType := ds.sessionTypes[collector.sessionType]
	if perType == nil {
		perType = map[string]int{}
		ds.sessionTypes[collector.sessionType] = perType
	}
	mapKey := collector.matches.toMapKey()
	count := perType[mapKey] + 1
	perType[mapKey] = count
	if count > collector.sessionMatcher.keepNSessionsPerScene {
		countlog.Debug("event!filtered_because_exceeded_limit", "sessionType", collector.sessionType)
		return nil
	}
	return collector.matches.ToScene()
}

var sessionTypeStart = []byte(`\x0c2DOCUMENT_URI`)
var sessionTypeEnd = []byte(`\x`)

var ExtractSessionType = func(input []byte) (string, error) {
	startPos := bytes.Index(input, sessionTypeStart)
	if startPos == -1 {
		return "", errors.New("session type start can not be found")
	}
	partialReq := input[startPos+len(sessionTypeStart):]
	endPos := bytes.Index(partialReq, sessionTypeEnd)
	if endPos == -1 {
		return "", errors.New("session type end can not be found")
	}
	return string(bytes.TrimSpace(partialReq[:endPos])), nil
}

type featureCollector struct {
	session        []byte
	iter           *jsoniter.Iterator
	sessionType    string
	matches        patternMatches
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
			sessionType, err := ExtractSessionType(req)
			if err != nil {
				if iter.Error == nil {
					iter.Error = err
				}
				return true
			}
			notifySessionTailer(sessionType, collector.session)
			collector.sessionType = sessionType
			sessionMatcher = getSessionMatcher(sessionType)
			if sessionMatcher != nil {
				collector.match(req, sessionMatcher.inboundRequestPg)
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
				collector.match(resp, collector.sessionMatcher.inboundResponsePg)
			}
		default:
			iter.Skip()
		}
		return true
	})
}

func (collector *featureCollector) colActions() {
	collector.iter.ReadArrayCB(func(iter *jsoniter.Iterator) bool {
		var srvCallOutboundMatcher *callOutboundMatcher
		wildcardCallOutboundMatcher := collector.sessionMatcher.callOutbounds["*"]
		iter.ReadObjectCB(func(iter *jsoniter.Iterator, field string) bool {
			switch field {
			case "ServiceName":
				serviceName := iter.ReadString()
				if collector.sessionMatcher != nil {
					srvCallOutboundMatcher = collector.sessionMatcher.callOutbounds[serviceName]
				}
			case "Request":
				req := []byte(iter.ReadString())
				if srvCallOutboundMatcher != nil {
					collector.match(req, srvCallOutboundMatcher.requestPg)
				}
				if wildcardCallOutboundMatcher != nil {
					collector.match(req, wildcardCallOutboundMatcher.requestPg)
				}
			case "Response":
				resp := []byte(iter.ReadString())
				if srvCallOutboundMatcher != nil {
					collector.match(resp, srvCallOutboundMatcher.responsePg)
				}
				if wildcardCallOutboundMatcher != nil {
					collector.match(resp, wildcardCallOutboundMatcher.responsePg)
				}
			default:
				iter.Skip()
			}
			return true
		})
		return true
	})
}

func (collector *featureCollector) match(bytes []byte, pg *patternGroup) {
	if pg == nil {
		return
	}
	matches, err := pg.match(bytes)
	if err != nil {
		countlog.Error("event!failed to match", "err", err)
	}
	collector.matches = append(collector.matches, matches...)
}
