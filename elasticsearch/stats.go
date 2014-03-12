package elasticsearch

import (
	"expvar"
)

var (
	bulkFull = expvar.NewInt("bulk full")
	bulkTime = expvar.NewInt("bulk time")
)
