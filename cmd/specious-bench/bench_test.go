package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStatsReports(t *testing.T) {
	assert := assert.New(t)
	now := time.Now()
	s := stats{
		Ops:   1000,
		Bytes: 1024 * 1024,
		Start: now.Add(-1 * time.Second),
		End:   &now,
	}
	assert.Equal(1.0, s.seconds())
	assert.Equal(1000.0, s.MicrosPerOp())
	assert.Equal(1.0, s.MegabytesPerSec())
}
