<p align="center">
<img 
    src="logo.png" 
    width="307" height="150" border="0" alt="BuntDB">
<br>
<a href="https://travis-ci.org/tidwall/buntdb"><img src="https://travis-ci.org/tidwall/buntdb.svg?branch=master" alt="Build Status"></a>
<img src="https://img.shields.io/badge/coverage-96%25-green.svg?style=flat" alt="Code Coverage">
<a href="https://godoc.org/github.com/tidwall/buntdb"><img src="https://godoc.org/github.com/tidwall/buntdb?status.svg" alt="GoDoc"></a>
<img src="https://img.shields.io/badge/version-0.1.0-green.svg" alt="Version">
</p>

====

BuntDB is a low-level, in-memory, key/value store in pure Go. 
It persists to disk, is ACID compliant, and uses locking for multiple
readers and a single writer. It supports custom indexes and geospatial 
data. It's ideal for projects that need a dependable database and favor 
speed over data size.

The desire to create BuntDB stems from the need for a new embeddable
database for [Tile38](https://github.com/tidwall/tile38). One that can work 
both as a performant [Raft Store](https://github.com/tidwall/raft-boltdb), 
and a Geospatial database.

Much of the API is inspired by the fantastic [BoltDB](https://github.com/boltdb/bolt),
an amazing key/value store that can handle terrabytes of data on disk.

Features
========

- In-memory database for [fast reads and writes](https://github.com/tidwall/raft-boltdb#benchmarks)
- Embeddable with a [simple API](https://godoc.org/github.com/tidwall/buntdb)
- [Spatial indexing](#spatial-indexes) for up to 4 dimensions; Useful for Geospatial data
- Create [custom indexes](#custom-indexes) for any data type
- [Built-in types](#built-in-types) that are easy to get up & running; String, Uint, Int, Float
- Flexible [iteration](#iterating) of data; ascending, descending, and ranges
- Durable append-only file format. Adopts the [Redis AOF](http://redis.io/topics/persistence) process
- Option to evict old items with an [expiration](#data-expiration) TTL
- Tight codebase, under 1K loc using the `cloc` command
- ACID semantics with locking [transactions](#transactions) that support rollbacks

Getting Started
===============

## Installing

To start using BuntDB, install Go and run `go get`:

```sh
$ go get github.com/tidwall/buntdb
```

This will retrieve the library.


## Opening a database

The primary object in BuntDB is a `DB`. To open or create your 
database, use the `buntdb.Open()` function:

```go
package main

import (
	"log"

	"github.com/tidwall/buntdb"
)

func main() {
	// Open the data.db file. It will be created if it doesn't exist.
	db, err := buntdb.Open("data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	... 
}
```

## Transactions
All reads and writes must be performed from inside a transaction. BuntDB can have one write transaction opened at a time, but can have many concurrent read transactions. Each transaction maintains a stable view of the database. In other words, once a transaction has begun, the data for that transaction cannot be changed by other transactions.

Transactions run in a function that exposes a `Tx` object, which represents the transaction state. While inside a transaction, all database operations should be performed using this object. You should never access the origin `DB` object while inside a transaction. Doing so may have side-effects, such as blocking your application.

When a transaction fails, it will roll back, and revert all chanages that ocurred to the database during that transaction. There's a single return value that you can use to close the transaction. For read/write transactions, returning an error this way will force the transaction to roll back. When a read/write transaction succeeds all changes are persisted to disk.

### Read-only Transactions
A read-only transaction should be used when you don't need to make changes to the data. The advantage of a read-only transaction is that there can be many running concurrently.

```go
err := db.View(func(tx *buntdb.Tx) error {
    ...
    return nil
})
```

### Read/write Transactions
A read/write transaction is used when you need to make changes to your data. There can only be one read/write transaction running at a time. So make sure you close it as soon as you are done with it. 

```go
err := db.Update(func(tx *buntdb.Tx) error {
    ...
    return nil
})
```

## Setting and getting key/values

To set a value you must open a read/write tranasction:

```go
err := db.Update(func(tx *buntdb.Tx) error {
    err := tx.Set("mykey", "myvalue", nil)
    return err
})
```


To get the value:

```go
err := db.View(func(tx *buntdb.Tx) error {
    val, err := tx.Get("mykey") 
    if err != nil{
    	return err
    }
    fmt.Printf("value is %s\n", val) 
    return nil
})
```

Getting non-existent values will case an `ErrNotFound` error.

### Iterating
All keys/value pairs are ordered in the database by the key. To iterate over the keys:

```go
err := db.View(func(tx *buntdb.Tx) error {
err := tx.Ascend("", func(key, value string) bool{
    	fmt.Printf("key: %s, value: %s\n", key, value)
    })
    return err
})
```

There is also `AscendGreaterOrEqual`, `AscendLessThan`, `AscendRange`, `Descend`, `DescendLessOrEqual`, `DescendGreaterThan`, and `DescendRange`. Please see the [documentation](https://godoc.org/github.com/tidwall/buntdb) for more information on these functions.


## Custom Indexes
Initially all data is stored in a single [B-tree](https://en.wikipedia.org/wiki/B-tree) with each item having one key and one value. All of these items are ordered by the key. This is great for quickly getting a value from a key or [iterating](#iterating) over the keys.

You can also create custom indexes that allow for ordering and [iterating](#iterating) over values. A custom index also uses a B-tree, but it's more flexible because it allows for custom ordering.

For example, let's say you want to create an index for ordering names:

```go
db.CreateIndex("names", "*", buntdb.IndexString)
```

This will create an index named `names` which stores and sorts all values. The second parameter is a pattern that is used to filter on keys. A `*` wildcard argument means that we want to accept all keys. `IndexString` is a built-in function that performs case-insensitive ordering on the values

Now you can add various names:

```go
db.Update(func(tx *buntdb.Tx) error {
    tx.Set("user:0:name", "tom", nil)
	tx.Set("user:1:name", "Randi", nil)
	tx.Set("user:2:name", "jane", nil)
	tx.Set("user:4:name", "Janet", nil)
	tx.Set("user:5:name", "Paula", nil)
	tx.Set("user:6:name", "peter", nil)
	tx.Set("user:7:name", "Terri", nil)
	return nil
})
```

Finally you can iterate over the index:

```go
db.View(func(tx *buntdb.Tx) error {
	tx.Ascend("names", func(key, val string) bool {
		fmt.Printf(buf, "%s %s\n", key, val)
		return true
	})
    return nil
})
```
The output should be:
```
user:2:name jane
user:4:name Janet
user:5:name Paula
user:6:name peter
user:1:name Randi
user:7:name Terri
user:0:name tom
```

The pattern parameter can be used to filter on keys like this:

```go
db.CreateIndex("names", "user:*", buntdb.IndexString)
```

Now only items with keys that have the prefix `user:` will be added to the `names` index.


### Built-in types
Along with `IndexString`, there is also `IndexInt`, `IndexUint`, and `IndexFloat`. 
These are built-in types for indexing. You can choose to use these or create your own.

So to create an index that is numerically ordered on an age key, we could use:

```go
db.CreateIndex("ages", "user:*:age", buntdb.IndexInt)
```

And then add values:

```go
db.Update(func(tx *buntdb.Tx) error {
    tx.Set("user:0:age", "35", nil)
	tx.Set("user:1:age", "49", nil)
	tx.Set("user:2:age", "13", nil)
	tx.Set("user:4:age", "63", nil)
	tx.Set("user:5:age", "8", nil)
	tx.Set("user:6:age", "3", nil)
	tx.Set("user:7:age", "16", nil)
	return nil
})
```

```go
db.View(func(tx *buntdb.Tx) error {
	tx.Ascend("ages", func(key, val string) bool {
		fmt.Printf(buf, "%s %s\n", key, val)
		return true
	})
    return nil
})
```

The output should be:
```
user:6:name 3
user:5:name 8
user:2:name 13
user:7:name 16
user:0:name 35
user:1:name 49
user:4:name 63
```

### Spatial Indexes
BuntDB has support for spatial indexes by storing rectangles in an [R-tree](https://en.wikipedia.org/wiki/R-tree). An R-tree is organized in a similar manner as a [B-tree](https://en.wikipedia.org/wiki/B-tree), and both are balanaced trees. But, an R-tree is special because it can operate on data that is in multiple dimensions. This is super handy for Geospatial applications.

To create a spatial index use the `CreateSpatialIndex` function:

```go
db.CreateSpatialIndex("fleet", "fleet:*:pos", buntdb.IndexRect)
```

Then `IndexRect` is a built-in function that converts rect strings to a format that the R-tree can use. It's easy to use this function out of the box, but you might find it better to create a custom one that renders from a different format, such as [Well-known text](https://en.wikipedia.org/wiki/Well-known_text) or [GeoJSON](http://geojson.org/).

To add some lon,lat points to the `fleet` index:

```go
db.Update(func(tx *buntdb.Tx) error {
    tx.Set("fleet:0:pos", "[-115.567 33.532]", nil)
	tx.Set("fleet:1:pos", "[-116.671 35.735]", nil)
	tx.Set("fleet:2:pos", "[-113.902 31.234]", nil)
	return nil
})
```

And then you can run the `Intersects` function on the index:

```go
db.View(func(tx *buntdb.Tx) error {
    tx.Intersects("fleet", "[-117 30],[-112 36]", func(key, val string) bool {
    	...
    	return true
    })
    return nil
})
```

This will get all three positions.

The bracket syntax `[-117 30],[-112 36]` is unique to BuntDB, and it's how the built-in rectangles are processed, but you are not limited to this syntax. Whatever Rect function you choose to use during `CreateSpatialIndex` will be used to process the paramater, in this case it's `IndexRect`.


### Data Expiration
Items can be automatically evicted by using the `SetOptions` object in the `Set` function to set a `TTL`.

```go
db.Update(func(tx *buntdb.Tx) error {
    tx.Set("mykey", "myval", &buntdb.SetOptions{Expires:true, TTL:time.Second})
	return nil
})
```

Now `mykey` will automatically be deleted after one second. You can remove the TTL by setting the value again with the same key/value, but with the options parameter set to nil.

## Contact
Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

BuntDB source code is available under the MIT [License](/LICENSE).
