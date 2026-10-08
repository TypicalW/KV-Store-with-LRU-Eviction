package kvstore

import (
	"time"
)

type syncMode int

const (
	syncAlways syncMode = iota
	syncInterval
	syncNever
)

type SyncPolicy struct {
	mode     syncMode
	interval time.Duration
}

var SyncAlways = SyncPolicy{mode: syncAlways}
var SyncNever = SyncPolicy{mode: syncNever}
