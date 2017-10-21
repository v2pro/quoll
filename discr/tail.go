package discr

import (
	"net/http"
	"time"
)

type tailedSession struct {
	sessionType string
	session     []byte
}

func Tail(respWriter http.ResponseWriter, sessionType string) {
	if len(sessionType) == 0 {
		sessionType = "*"
	}
	sessionChannel := make(chan tailedSession)
	tailer := func(sessionType string, session []byte) {
		sessionChannel <- tailedSession{sessionType: sessionType, session: session}
	}
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
			_, err = respWriter.Write([]byte(tailedSession.sessionType))
			if err != nil {
				return
			}
			_, err = respWriter.Write([]byte("<br>\n"))
			if err != nil {
				return
			}
			if f, ok := respWriter.(http.Flusher); ok {
				f.Flush()
			}
		}
	}
}
