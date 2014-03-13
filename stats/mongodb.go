package stats

import (
	"expvar"
)

var Finds = expvar.NewInt("number of find() required")
