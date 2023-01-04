package types

import (
	"testing"
	"time"
)

const (
	nDaySeconds = 24 * 3600
)

func TestRPCURLInfoBuckets(t *testing.T) {
	nowSecs := time.Now().Unix()
	var prevBucket int64 = -1
	for i := 0; i < nDaySeconds*4; i++ {
		bucket := NewRecordRPCURLInfo().getBucket(nowSecs + 1)
		if prevBucket == -1 {
			prevBucket = bucket
			continue
		}
		differ := bucket - prevBucket
		if bucket != prevBucket && differ != 1 && differ != -29 {
			t.Fatalf("currect bucket %d, prev bucket %d", bucket, prevBucket)
		}
		if prevBucket != bucket {
			t.Logf("got new bucket %d", bucket)
		}
		nowSecs++
		prevBucket = bucket
	}
}

func TestRPCURLBucketPeriods(t *testing.T) {
	nowSecs := time.Now().Unix()
	bucket := &RecordRPCURLBucket{}
	periods := make(map[string]bool)
	for i := 0; i < nDaySeconds; i++ {
		period := bucket.getPeriod(nowSecs)
		periods[period] = true
		// t.Logf("period %s", period)
		nowSecs++
	}

	nDayPeriods := nDaySeconds/60 + 1
	if len(periods) != nDayPeriods {
		t.Fatalf("expected %d periods but got %d", nDayPeriods, len(periods))
	}

	nowSecs = time.Now().Unix()

	for i := 0; i < 4; i++ {
		period := bucket.getPeriod(nowSecs)
		t.Logf("period %s", period)
		nowSecs = nowSecs + 60
	}
}
