package discr

import (
	"net/http"
	"time"
)

type tailedSession struct {
	sessionType string
	session     []byte
}

func Tail(respWriter http.ResponseWriter, sessionType string, showSession bool, limit int) {
	if len(sessionType) == 0 {
		sessionType = "*"
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
				return
			}
			if _, err = respWriter.Write([]byte(tailedSession.sessionType)); err != nil {
				return
			}
			if _, err = respWriter.Write([]byte("</span><br/>\n")); err != nil {
				return
			}
			if showSession {
				if _, err = respWriter.Write([]byte("<pre>\n")); err != nil {
					return
				}
				if _, err = respWriter.Write(tailedSession.session); err != nil {
					return
				}
				if _, err = respWriter.Write([]byte("</pre><br/>\n")); err != nil {
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
