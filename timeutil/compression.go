package timeutil

import (
	"time"
	"fmt"
	"math"
)

func Compress(base time.Time, now time.Time) uint32 {
	duration := now.Sub(base)
	if duration < 0 {
		panic(fmt.Sprintf("can not compress timestamp: %s < %v", now, base))
	}
	asMilli := duration >> 10
	if asMilli > math.MaxUint32 {
		panic(fmt.Sprintf("can not compress timestamp: %v is too large", duration))
	}
	return uint32(asMilli)
}

func Uncompress(base time.Time, compressed uint32) time.Time {
	return base.Add(time.Duration(compressed) << 10)
}
