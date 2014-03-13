package stats

import (
	"expvar"
)

var (
	Unsets   = expvar.NewInt("Total $unset")
	Sets     = expvar.NewInt("Total $set")
	Complete = expvar.NewInt("Total complete objects")
)
