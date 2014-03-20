# [CryRiver](http://www.youtube.com/watch?v=DksSPZTZES0) [![GoDoc](https://godoc.org/github.com/duego/cryriver?status.png)](https://godoc.org/github.com/duego/cryriver)

Cryriver syncs changes from MongoDB to Elasticsearch.

# Operation

The cryriver is designed to run one process on each MongoDB shard, they should preferably run on each primary to keep the changes from lagging behind too much.

It can target any of the available ES nodes in the cluster, for example on ShardA primary:

```
cryriver -concurrency=2 -cpu=1 -debug=0.0.0.0:8080 -es=http://10.70.1.148:9200 -index=duego -ns=duego.users -initial=true
```

On ShardB primary:

```
cryriver -concurrency=2 -cpu=1 -debug=0.0.0.0:8080 -es=http://10.70.1.127:9200 -index=duego -ns=duego.users -initial=true
```

We will now divide all incoming updates on two nodes in the ES cluster.

**concurrency** Is how many simultaneous bulk requests we will allow  
**cpu** Is how many CPU cores we allow Go to utilize, it's not always beneficial to set this to the number of available cores  
**debug** Is used for profiling and listing exported variables (see below)  
**es** Specifies which ES node to send bulk requests to  
**index** What ES index to use  
**ns** The namespace on MongoDB to tail from oplog, it's in the format of database.collection  
**initial** Set this to true to perform the initial reading of all documents on the collection before starting to tail the oplog

# Changing values before hitting ES

One way of attaching your custom functions to manipulate the outgoing data like this:

```Go
func init() {
	mongodb.DefaultManipulators = append(
		mongodb.DefaultManipulators,
		[]mongodb.Manipulator{
			mongodb.ManipulateFunc(Mapper),
		},
	)
}

// Mapper transforms {"this.kind.of.keys": "value"} into {"this": {"kind": {"of": {"keys": "value"}}}}
func Mapper(doc *bson.M, op mongodb.OplogOperation) error {
	d := make(map[string]interface{})
	defer func() { *doc = d }()

	for key, value := range *doc {
		parts := strings.Split(key, ".")
		// Create the string map structure up to the last part which will contain the value
		root := d
		for _, part := range parts[:len(parts)-1] {
			root[part] = make(map[string]interface{})
			root = root[part].(map[string]interface{})
		}
		// Set the value of the last key
		root[parts[len(parts)-1:][0]] = value
	}
	return nil
}
```

Then include your custom package by creating a new file in the main package linking to yours:

```
package main

import (
	_ "github.com/your/private/package"
)
```

# Profiling / Debug vars

A few variables is exposed for listing the progress of the river, for example what the latest oplog timestamp we have sent to ES is.
This can be listed on the chosen debug address, for example http://localhost:8080/debug/vars

Live profiling can be performed with no noticeable performance impact on the same address.
For example to show CPU usage:

```
go tool pprof http://localhost:8080/debug/pprof/profile --seconds=300
Read http://localhost:8080/debug/pprof/symbol
Gathering CPU profile from http://localhost:8080/debug/pprof/profile?seconds=300 for 300 seconds to
  /var/folders/2m/qrwd506x4wlbf97dy_g5tf9m0000gn/T/e90smSiKnp
Be patient...
Wrote profile to /var/folders/2m/qrwd506x4wlbf97dy_g5tf9m0000gn/T/e90smSiKnp
Welcome to pprof!  For help, type 'help'.
(pprof) top
Total: 67 samples
      10  14.9%  14.9%       10  14.9% runtime.futex
       7  10.4%  25.4%        7  10.4% syscall.Syscall
       6   9.0%  34.3%        6   9.0% sweepspan
       3   4.5%  38.8%        3   4.5% runtime.xchg
       2   3.0%  41.8%        3   4.5% github.com/your/private/package.Mapper
       2   3.0%  44.8%        3   4.5% labix.org/v2/mgo.func·009
       2   3.0%  47.8%        7  10.4% runtime.mallocgc
       2   3.0%  50.7%        2   3.0% runtime.settype_flush
       1   1.5%  52.2%        1   1.5% MCentral_Grow
       1   1.5%  53.7%        1   1.5% MHeap_FreeLocked
```

# FAQ

## Starting up takes forever

Initial cursor to tail oplog can take > 30 min and is because MongoDB scans through the whole table to resume at the timestamp we give it. Once this is done, the lag should be at most 1 second.

## How do I resume operations after a restart

The river will keep track of the latest timestamp it saw and save it to a file, if -initial=false is given it will use this timestamp for creating the cursor on the oplog and resume updating the difference from when it last stopped. If it has been down for some time, the initial scan of updates will consume more CPU until it has catched up.

## I need to debug or fix one of the shards, what now?

It's safe to stop or start cryrivers on each separate shard without affecting the others.
