package mongodb

import (
	"flag"
	"fmt"
	"labix.org/v2/mgo"
	"log"
	"sync"
)

var (
	integration = flag.Bool("integration", false, "Should we run integration tests that requires external dependencies such as mongodb?")
	mongodb     = flag.String("mongodb", "192.168.100.100", "MongoDB server to use for tests")
	db          = flag.String("db", "test", "Database name to use for tests, same for ES and MongoDB")
)

var (
	session  *mgo.Session
	initOnce sync.Once
)

func initSession() {
	initOnce.Do(func() {
		var err error
		session, err = mgo.Dial(fmt.Sprintf("%s/%s", *mongodb, *db))
		if err != nil {
			log.Fatal(err)
		}
	})
}
