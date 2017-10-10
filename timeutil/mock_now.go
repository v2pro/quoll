package timeutil

import "time"

var mockedNow *time.Time

func Now() time.Time {
	if mockedNow != nil {
		return *mockedNow
	}
	return time.Now()
}

func MockNow(now time.Time) {
	mockedNow = &now
}
