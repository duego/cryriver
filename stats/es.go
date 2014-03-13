package stats

import (
	"expvar"
)

var (
	BulkFull = expvar.NewInt("bulk full")
	BulkTime = expvar.NewInt("bulk time")
)
