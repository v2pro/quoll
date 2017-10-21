package discr

import (
	"net/http"
	"time"
	"github.com/v2pro/plz/countlog"
	"github.com/json-iterator/go"
)

type tailedSession struct {
	sessionType string
	session     []byte
}

func Tail(respWriter http.ResponseWriter, sessionType string, showSession bool, limit int, cnf SessionMatcherCnf) {
	if len(sessionType) == 0 {
		sessionType = "*"
	}
	sessionMatcher, err := newSessionMatcher(cnf)
	if err != nil {
		countlog.Error("event!tail.err", "err", err)
		return
	}
	sessionChannel := make(chan tailedSession)
	tailer := func(sessionType string, session []byte) {
		sessionChannel <- tailedSession{sessionType: sessionType, session: session}
	}
	count := 0
	for {
		err := AddSessionTailer(sessionType, tailer)
		if err != nil {
			respWriter.Write([]byte("overflow!!!\n"))
			return
		}
		timer := time.NewTimer(time.Minute)
		select {
		case <-timer.C:
			respWriter.Write([]byte("timeout!!!\n"))
			return
		case tailedSession := <-sessionChannel:
			if _, err = respWriter.Write([]byte(`<span style="color:red;">`)); err != nil {
				countlog.Error("event!tail.err", "err", err)
				return
			}
			if _, err = respWriter.Write([]byte(tailedSession.sessionType)); err != nil {
				countlog.Error("event!tail.err", "err", err)
				return
			}
			if _, err = respWriter.Write([]byte("</span><br/>\n")); err != nil {
				countlog.Error("event!tail.err", "err", err)
				return
			}
			matches, err := tryMatcher(tailedSession.session, sessionMatcher)
			if err != nil {
				if _, err = respWriter.Write([]byte(err.Error() + "<br/>")); err != nil {
					countlog.Error("event!tail.err", "err", err)
					return
				}
			} else if matches != nil {
				if _, err = respWriter.Write([]byte(`<span style="color:blue;">`)); err != nil {
					countlog.Error("event!tail.err", "err", err)
					return
				}
				for k, v := range matches.ToScene().ToMap() {
					if _, err = respWriter.Write([]byte(k + " => " + v + "<br/>")); err != nil {
						countlog.Error("event!tail.err", "err", err)
						return
					}
				}
				if _, err = respWriter.Write([]byte("</span><br/>\n")); err != nil {
					countlog.Error("event!tail.err", "err", err)
					return
				}
			}
			if showSession {
				if _, err = respWriter.Write([]byte("<pre>\n")); err != nil {
					countlog.Error("event!tail.err", "err", err)
					return
				}
				if _, err = respWriter.Write(tailedSession.session); err != nil {
					countlog.Error("event!tail.err", "err", err)
					return
				}
				if _, err = respWriter.Write([]byte("</pre><br/>\n")); err != nil {
					countlog.Error("event!tail.err", "err", err)
					return
				}
			}
			if f, ok := respWriter.(http.Flusher); ok {
				f.Flush()
			}
			count++
			if limit > 0 && count > limit {
				respWriter.Write([]byte("limit reached!!!\n"))
				return
			}
		}
	}
}

func tryMatcher(session []byte, matcher *sessionMatcher) (patternMatches, error) {
	iter := jsoniter.ConfigFastest.BorrowIterator(session)
	defer jsoniter.ConfigFastest.ReturnIterator(iter)
	collector := &featureCollector{iter: iter, session: session, sessionMatcher: matcher, noTail:true}
	collector.colSession()
	if iter.Error != nil {
		return nil, iter.Error
	}
	return collector.matches, nil
}
