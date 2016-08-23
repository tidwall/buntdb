// Package buntdb implements a low-level in-memory key/value store in pure Go.
// It persists to disk, is ACID compliant, and uses locking for multiple
// readers and a single writer. Bunt is ideal for projects that need
// a dependable database, and favor speed over data size.
package buntdb

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/gjson"
	"github.com/tidwall/rtree"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an item or index is not in the database.
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an index already exists in the database.
	ErrIndexExists = errors.New("index exists")

	// ErrInvalidOperation is returned when an operation cannot be completed.
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is returned when a shrink operation is in-process.
	ErrShrinkInProcess = errors.New("shrink is in-process")
)

// DB represents a collection of key-value pairs that persist on disk.
// Transactions are used for all forms of data access to the DB.
type DB struct {
	mu        sync.RWMutex      // the gatekeeper for all fields
	file      *os.File          // the underlying file
	buf       *bytes.Buffer     // a buffer to write to
	keys      *btree.BTree      // a tree of all item ordered by key
	exps      *btree.BTree      // a tree of items ordered by expiration
	idxs      map[string]*index // the index trees.
	exmgr     bool              // indicates that expires manager is running.
	flushes   int               // a count of the number of disk flushes
	closed    bool              // set when the database has been closed
	config    Config            // the database configuration
	persist   bool              // do we write to disk
	shrinking bool              // when an aof shrink is in-process.
	lastaofsz int               // the size of the last shrink aof size
}

// SyncPolicy represents how often data is synced to disk.
type SyncPolicy int

const (
	// Never is used to disable syncing data to disk.
	// The faster and less safe method.
	Never SyncPolicy = 0
	// EverySecond is used to sync data to disk every second.
	// It's pretty fast and you can lose 1 second of data if there
	// is a disaster.
	// This is the recommended setting.
	EverySecond = 1
	// Always is used to sync data after every write to disk.
	// Slow. Very safe.
	Always = 2
)

// Config represents database configuration options. These
// options are used to change various behaviors of the database.
type Config struct {
	// SyncPolicy adjusts how often the data is synced to disk.
	// This value can be Never, EverySecond, or Always.
	// The default is EverySecond.
	SyncPolicy SyncPolicy

	// AutoShrinkPercentage is used by the background process to trigger
	// a shrink of the aof file when the size of the file is larger than the
	// percentage of the result of the previous shrunk file.
	// For example, if this value is 100, and the last shrink process
	// resulted in a 100mb file, then the new aof file must be 200mb before
	// a shrink is triggered.
	AutoShrinkPercentage int

	// AutoShrinkMinSize defines the minimum size of the aof file before
	// an automatic shrink can occur.
	AutoShrinkMinSize int

	// AutoShrinkDisabled turns off automatic background shrinking
	AutoShrinkDisabled bool
}

// exctx is a simple b-tree context for ordering by expiration.
type exctx struct {
	db *DB
}

// Default number of btree degrees
const btreeDegrees = 64

// Open opens a database at the provided path.
// If the file does not exist then it will be created automatically.
func Open(path string) (*DB, error) {
	db := &DB{}
	db.keys = btree.New(btreeDegrees, nil)
	db.exps = btree.New(btreeDegrees, &exctx{db})
	db.idxs = make(map[string]*index)
	db.buf = &bytes.Buffer{}
	db.config = Config{
		SyncPolicy:           EverySecond,
		AutoShrinkPercentage: 100,
		AutoShrinkMinSize:    32 * 1024 * 1024,
	}
	db.persist = path != ":memory:"
	if db.persist {
		var err error
		// Hardcoding 0666 as the default mode.
		db.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		if err := db.load(); err != nil {
			_ = db.file.Close()
			return nil, err
		}
	}
	// start the background manager.
	go db.backgroundManager()
	return db, nil
}

// Close releases all database resources.
// All transactions must be closed before closing the database.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.closed = true
	if db.persist {
		db.file.Sync() // do a sync but ignore the error
		if err := db.file.Close(); err != nil {
			return err
		}
	}
	// Let's release all references to nil. This will help both with debugging
	// late usage panics and it provides a hint to the garbage collector
	db.keys, db.exps, db.idxs, db.file = nil, nil, nil, nil
	return nil
}

// index represents a b-tree or r-tree index and also acts as the
// b-tree/r-tree context for itself.
type index struct {
	btr     *btree.BTree                           // contains the items
	rtr     *rtree.RTree                           // contains the items
	name    string                                 // name of the index
	pattern string                                 // a required key pattern
	less    func(a, b string) bool                 // less comparison function
	rect    func(item string) (min, max []float64) // rect from string function
	db      *DB                                    // the origin database
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
func (db *DB) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.createIndex(name, pattern, less, nil)
}

// CreateSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// An error will occur if an index with the same name already exists.
//
// The rect function converts a string to a rectangle. The rectangle is
// represented by two arrays, min and max. Both arrays may have a length
// between 1 and 20, and both arrays must match in length. A length of 1 is a
// one dimensional rectangle, and a length of 4 is a four dimension rectangle.
// There is support for up to 20 dimensions.
// The values of min must be less than the values of max at the same dimension.
// Thus min[0] must be less-than-or-equal-to max[0].
// The IndexRect is a default function that can be used for the rect
// parameter.
func (db *DB) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.createIndex(name, pattern, nil, rect)
}

// createIndex is called by CreateIndex() and CreateSpatialIndex()
func (db *DB) createIndex(
	name string,
	pattern string,
	lessers []func(a, b string) bool,
	rect func(item string) (min, max []float64),
) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	if name == "" {
		return ErrIndexExists
	}
	if _, ok := db.idxs[name]; ok {
		return ErrIndexExists
	}
	var less func(a, b string) bool
	switch len(lessers) {
	default:
		less = func(a, b string) bool {
			for i := 0; i < len(lessers)-1; i++ {
				if lessers[i](a, b) {
					return true
				}
				if lessers[i](b, a) {
					return false
				}
			}
			return lessers[len(lessers)-1](a, b)
		}
	case 0:
		less = func(a, b string) bool { return false }
	case 1:
		less = lessers[0]
	}
	idx := &index{
		name:    name,
		pattern: pattern,
		less:    less,
		rect:    rect,
		db:      db,
	}
	if less != nil {
		idx.btr = btree.New(btreeDegrees, idx)
	}
	if rect != nil {
		idx.rtr = rtree.New(idx)
	}

	db.keys.Ascend(func(item btree.Item) bool {
		dbi := item.(*dbItem)
		if !wildcardMatch(dbi.key, idx.pattern) {
			return true
		}
		if less != nil {
			idx.btr.ReplaceOrInsert(dbi)
		}
		if rect != nil {
			idx.rtr.Insert(dbi)
		}
		return true
	})
	db.idxs[name] = idx
	return nil
}

// wilcardMatch returns true if str matches pattern. This is a very
// simple wildcard match where '*' matches on any number characters
// and '?' matches on any one character.
func wildcardMatch(str, pattern string) bool {
	if pattern == "*" {
		return true
	}
	return deepMatch(str, pattern)
}
func deepMatch(str, pattern string) bool {
	for len(pattern) > 0 {
		switch pattern[0] {
		default:
			if len(str) == 0 || str[0] != pattern[0] {
				return false
			}
		case '?':
			if len(str) == 0 {
				return false
			}
		case '*':
			return wildcardMatch(str, pattern[1:]) ||
				(len(str) > 0 && wildcardMatch(str[1:], pattern))
		}
		str = str[1:]
		pattern = pattern[1:]
	}
	return len(str) == 0 && len(pattern) == 0
}

// DropIndex removes an index.
func (db *DB) DropIndex(name string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	if name == "" {
		return ErrInvalidOperation
	}
	if _, ok := db.idxs[name]; !ok {
		return ErrNotFound
	}
	delete(db.idxs, name)
	return nil
}

// Indexes returns a list of index names.
func (db *DB) Indexes() ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrDatabaseClosed
	}
	names := make([]string, 0, len(db.idxs))
	for name := range db.idxs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// ReadConfig returns the database configuration.
func (db *DB) ReadConfig(config *Config) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	*config = db.config
	return nil
}

// SetConfig updates the database configuration.
func (db *DB) SetConfig(config Config) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	switch config.SyncPolicy {
	default:
		return ErrInvalidSyncPolicy
	case Never, EverySecond, Always:
	}
	db.config = config
	return nil
}

// insertIntoDatabase performs inserts an item in to the database and updates
// all indexes. If a previous item with the same key already exists, that item
// will be replaced with the new one, and return the previous item.
func (db *DB) insertIntoDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev := db.keys.ReplaceOrInsert(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// fully delete this item from all indexes.
		pdbi = prev.(*dbItem)
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				idx.rtr.Remove(pdbi)
			}
		}
	}
	if item.opts != nil && item.opts.ex {
		// The new item has eviction options. Add it to the
		// expires tree
		db.exps.ReplaceOrInsert(item)
	}
	for _, idx := range db.idxs {
		if !wildcardMatch(item.key, idx.pattern) {
			continue
		}
		if idx.btr != nil {
			// Add new item to btree index.
			idx.btr.ReplaceOrInsert(item)
		}
		if idx.rtr != nil {
			// Add new item to rtree index.
			idx.rtr.Insert(item)
		}
	}
	// we must return the previous item to the caller.
	return pdbi
}

// deleteFromDatabase removes and item from the database and indexes. The input
// item must only have the key field specified thus "&dbItem{key: key}" is all
// that is needed to fully remove the item with the matching key. If an item
// with the matching key was found in the database, it will be removed and
// returned to the caller. A nil return value means that the item was not
// found in the database
func (db *DB) deleteFromDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev := db.keys.Delete(item)
	if prev != nil {
		pdbi = prev.(*dbItem)
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the exipres tree.
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				idx.btr.Delete(pdbi)
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				idx.rtr.Remove(pdbi)
			}
		}
	}
	return pdbi
}

// backgroundManager runs continuously in the background and performs various
// operations such as removing expired items and syncing to disk.
func (db *DB) backgroundManager() {
	flushes := 0
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		var shrink bool
		// Open a standard view. This will take a full lock of the
		// database thus allowing for access to anything we need.
		err := db.Update(func(tx *Tx) error {
			if db.persist && !db.config.AutoShrinkDisabled {
				pos, err := db.file.Seek(0, 1)
				if err != nil {
					return err
				}
				aofsz := int(pos)
				if aofsz > db.config.AutoShrinkMinSize {
					prc := float64(db.config.AutoShrinkPercentage) / 100.0
					shrink = aofsz > db.lastaofsz+int(float64(db.lastaofsz)*prc)
				}
			}
			// produce a list of expired items that need removing
			var remove []*dbItem
			db.exps.AscendLessThan(&dbItem{
				opts: &dbItemOpts{ex: true, exat: time.Now()},
			}, func(item btree.Item) bool {
				remove = append(remove, item.(*dbItem))
				return true
			})
			for _, item := range remove {
				if _, err := tx.Delete(item.key); err != nil {
					// it's ok to get a "not found" because the
					// 'Delete' method reports "not found" for
					// expired items.
					if err != ErrNotFound {
						return err
					}
				}
			}

			// execute a disk sync.
			if db.persist && db.config.SyncPolicy == EverySecond &&
				flushes != db.flushes {
				_ = db.file.Sync()
				flushes = db.flushes
			}
			return nil
		})
		if err == ErrDatabaseClosed {
			break
		}
		if shrink {
			if err = db.Shrink(); err != nil {
				if err == ErrDatabaseClosed {
					break
				}
			}
		}
	}
}

// Shrink will make the database file smaller by removing redundant
// log entries. This operation does not block the database.
func (db *DB) Shrink() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}
	if !db.persist {
		// The database was opened with ":memory:" as the path.
		// There is no persistence, and no need to do anything here.
		db.mu.Unlock()
		return nil
	}
	if db.shrinking {
		// The database is already in the process of shrinking.
		db.mu.Unlock()
		return ErrShrinkInProcess
	}
	db.shrinking = true
	defer func() {
		db.mu.Lock()
		db.shrinking = false
		db.mu.Unlock()
	}()
	fname := db.file.Name()
	tmpname := fname + ".tmp"
	// the endpos is used to return to the end of the file when we are
	// finished writing all of the current items.
	endpos, err := db.file.Seek(0, 2)
	if err != nil {
		return err
	}
	db.mu.Unlock()
	time.Sleep(time.Second / 4) // wait just a bit before starting
	f, err := os.Create(tmpname)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(tmpname)
	}()

	// we are going to read items in as chunks as to not hold up the database
	// for too long.
	buf := &bytes.Buffer{}
	pivot := ""
	done := false
	for !done {
		err := func() error {
			db.mu.RLock()
			defer db.mu.RUnlock()
			if db.closed {
				return ErrDatabaseClosed
			}
			n := 0
			done = true
			db.keys.AscendGreaterOrEqual(&dbItem{key: pivot},
				func(item btree.Item) bool {
					dbi := item.(*dbItem)
					if n > 100 {
						pivot = dbi.key
						done = false
						return false
					}
					dbi.writeSetTo(buf)
					n++
					return true
				},
			)
			if _, err := f.Write(buf.Bytes()); err != nil {
				return err
			}
			buf.Reset()
			return nil
		}()
		if err != nil {
			return err
		}
	}
	// We reached this far so all of the items have been written to a new tmp
	// There's some more work to do by appending the new line from the aof
	// to the tmp file and finally swap the files out.
	return func() error {
		// We're wrapping this in a function to get the benefit of a defered
		// lock/unlock.
		db.mu.Lock()
		defer db.mu.Unlock()
		if db.closed {
			return ErrDatabaseClosed
		}
		// We are going to open a new version of the aof file so that we do
		// not change the seek position of the previous. This may cause a
		// problem in the future if we choose to use syscall file locking.
		aof, err := os.Open(fname)
		if err != nil {
			return err
		}
		defer func() { _ = aof.Close() }()
		if _, err := aof.Seek(endpos, 0); err != nil {
			return err
		}
		// Just copy all of the new commands that have occurred since we
		// started the shrink process.
		if _, err := io.Copy(f, aof); err != nil {
			return err
		}
		// Close all files
		if err := aof.Close(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := db.file.Close(); err != nil {
			return err
		}
		// Any failures below here is really bad. So just panic.
		if err := os.Rename(tmpname, fname); err != nil {
			panic(err)
		}
		db.file, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
		pos, err := db.file.Seek(0, 2)
		if err != nil {
			return err
		}
		db.lastaofsz = int(pos)
		return nil
	}()
}

var errValidEOF = errors.New("valid eof")

// load reads entries from the append only database file and fills the database.
// The file format uses the Redis append only file format, which is and a series
// of RESP commands. For more information on RESP please read
// http://redis.io/topics/protocol. The only supported RESP commands are DEL and
// SET.
func (db *DB) load() error {
	fi, err := db.file.Stat()
	if err != nil {
		return err
	}
	modTime := fi.ModTime()
	data := make([]byte, 4096)
	parts := make([]string, 0, 8)
	r := bufio.NewReader(db.file)
	for {
		// read a single command.
		// first we should read the number of parts that the of the command
		line, err := r.ReadBytes('\n')
		if err != nil {
			if len(line) > 0 {
				// got an eof but also data. this should be an unexpected eof.
				return io.ErrUnexpectedEOF
			}
			if err == io.EOF {
				break
			}
			return err
		}
		if line[0] != '*' {
			return ErrInvalid
		}
		// convert the string number to and int
		var n int
		if len(line) == 4 && line[len(line)-2] == '\r' {
			if line[1] < '0' || line[1] > '9' {
				return ErrInvalid
			}
			n = int(line[1] - '0')
		} else {
			if len(line) < 5 || line[len(line)-2] != '\r' {
				return ErrInvalid
			}
			for i := 1; i < len(line)-2; i++ {
				if line[i] < '0' || line[i] > '9' {
					return ErrInvalid
				}
				n = n*10 + int(line[i]-'0')
			}
		}
		// read each part of the command.
		parts = parts[:0]
		for i := 0; i < n; i++ {
			// read the number of bytes of the part.
			line, err := r.ReadBytes('\n')
			if err != nil {
				return err
			}
			if line[0] != '$' {
				return ErrInvalid
			}
			// convert the string number to and int
			var n int
			if len(line) == 4 && line[len(line)-2] == '\r' {
				if line[1] < '0' || line[1] > '9' {
					return ErrInvalid
				}
				n = int(line[1] - '0')
			} else {
				if len(line) < 5 || line[len(line)-2] != '\r' {
					return ErrInvalid
				}
				for i := 1; i < len(line)-2; i++ {
					if line[i] < '0' || line[i] > '9' {
						return ErrInvalid
					}
					n = n*10 + int(line[i]-'0')
				}
			}
			// resize the read buffer
			if len(data) < n+2 {
				dataln := len(data)
				for dataln < n+2 {
					dataln *= 2
				}
				data = make([]byte, dataln)
			}
			if _, err = io.ReadFull(r, data[:n+2]); err != nil {
				return err
			}
			if data[n] != '\r' || data[n+1] != '\n' {
				return ErrInvalid
			}
			// copy string
			parts = append(parts, string(data[:n]))
		}
		// finished reading the command

		if len(parts) == 0 {
			continue
		}
		if len(parts[0]) != 3 {
			return ErrInvalid
		}
		if (parts[0][0] == 's' || parts[0][1] == 'S') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 't' || parts[0][2] == 'T') {
			// SET
			if len(parts) < 3 || len(parts) == 4 || len(parts) > 5 {
				return ErrInvalid
			}
			if len(parts) == 5 {
				if strings.ToLower(parts[3]) != "ex" {
					return ErrInvalid
				}
				ex, err := strconv.ParseInt(parts[4], 10, 64)
				if err != nil {
					return err
				}
				now := time.Now()
				dur := (time.Duration(ex) * time.Second) - now.Sub(modTime)
				if dur > 0 {
					db.insertIntoDatabase(&dbItem{
						key: parts[1],
						val: parts[2],
						opts: &dbItemOpts{
							ex:   true,
							exat: now.Add(dur),
						},
					})
				}
			} else {
				db.insertIntoDatabase(&dbItem{key: parts[1], val: parts[2]})
			}
		} else if (parts[0][0] == 'd' || parts[0][1] == 'D') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 'l' || parts[0][2] == 'L') {
			// DEL
			if len(parts) != 2 {
				return ErrInvalid
			}
			db.deleteFromDatabase(&dbItem{key: parts[1]})
		} else {
			return ErrInvalid
		}
	}
	pos, err := db.file.Seek(0, 2)
	if err != nil {
		return err
	}
	db.lastaofsz = int(pos)
	return nil
}

// managed calls a block of code that is fully contained in a transaction.
// This method is intended to be wrapped by Update and View
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = db.begin(writable)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			// The caller returned an error. We must rollback.
			_ = tx.rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			err = tx.commit()
		} else {
			// read-only transaction can only roll back.
			err = tx.rollback()
		}
	}()
	tx.funcd = true
	defer func() {
		tx.funcd = false
	}()
	err = fn(tx)
	return
}

// View executes a function within a managed read-only transaction.
// When a non-nil error is returned from the function that error will be return
// to the caller of View().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// Update executes a function within a managed read/write transaction.
// The transaction has been committed when no error is returned.
// In the event that an error is returned, the transaction will be rolled back.
// When a non-nil error is returned from the function, the transaction will be
// rolled back and the that error will be return to the caller of Update().
//
// Executing a manual commit or rollback from inside the function will result
// in a panic.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// get return an item or nil if not found.
func (db *DB) get(key string) *dbItem {
	item := db.keys.Get(&dbItem{key: key})
	if item != nil {
		return item.(*dbItem)
	}
	return nil
}

// Tx represents a transaction on the database. This transaction can either be
// read-only or read/write. Read-only transactions can be used for retrieving
// values for keys and iterating through keys and values. Read/write
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
type Tx struct {
	db        *DB                // the underlying database.
	writable  bool               // when false mutable operations fail.
	funcd     bool               // when true Commit and Rollback panic.
	rollbacks map[string]*dbItem // cotnains details for rolling back tx.
	commits   map[string]*dbItem // contains details for committing tx.
}

// begin opens a new transaction.
// Multiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
//
// All transactions must be closed by calling Commit() or Rollback() when done.
func (db *DB) begin(writable bool) (*Tx, error) {
	tx := &Tx{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closed {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	if writable {
		tx.rollbacks = make(map[string]*dbItem)
		if db.persist {
			tx.commits = make(map[string]*dbItem)
		}
	}
	return tx, nil
}

// lock locks the database based on the transaction type.
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// rollbackInner handles the underlying rollback logic.
// Intended to be called from Commit() and Rollback().
func (tx *Tx) rollbackInner() {
	for key, item := range tx.rollbacks {
		tx.db.deleteFromDatabase(&dbItem{key: key})
		if item != nil {
			// When an item is not nil, we will need to reinsert that item
			// into the database overwriting the current one.
			tx.db.insertIntoDatabase(item)
		}
	}
}

// commit writes all changes to disk.
// An error is returned when a write error occurs, or when a Commit() is called
// from a read-only transaction.
func (tx *Tx) commit() error {
	if tx.funcd {
		panic("managed tx commit not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}
	var err error
	if tx.db.persist && len(tx.commits) > 0 {
		// Each committed record is written to disk
		tx.db.buf.Reset()
		for key, item := range tx.commits {
			if item == nil {
				(&dbItem{key: key}).writeDeleteTo(tx.db.buf)
			} else {
				item.writeSetTo(tx.db.buf)
			}
		}
		// Flushing the buffer only once per transaction.
		// If this operation fails then the write did failed and we must
		// rollback.
		if _, err = tx.db.file.Write(tx.db.buf.Bytes()); err != nil {
			tx.rollbackInner()
		}
		if tx.db.config.SyncPolicy == Always {
			_ = tx.db.file.Sync()
		}
		// Increment the number of flushes. The background syncing uses this.
		tx.db.flushes++

	}
	// Unlock the database and allow for another writable transaction.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return err
}

// rollback closes the transaction and reverts all mutable operations that
// were performed on the transaction such as Set() and Delete().
//
// Read-only transactions can only be rolled back, not committed.
func (tx *Tx) rollback() error {
	if tx.funcd {
		panic("managed tx rollback not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	}
	// The rollback func does the heavy lifting.
	if tx.writable {
		tx.rollbackInner()
	}
	// unlock the database for more transactions.
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	tx.db = nil
	return nil
}

// dbItemOpts holds various meta information about an item.
type dbItemOpts struct {
	ex   bool      // does this item expire?
	exat time.Time // when does this item expire?
}
type dbItem struct {
	key, val string      // the binary key and value
	opts     *dbItemOpts // optional meta information
}

// writeHead writes the resp header part
func writeHead(wr *bytes.Buffer, c byte, n int) {
	_ = wr.WriteByte(c)
	_, _ = wr.WriteString(strconv.FormatInt(int64(n), 10))
	_, _ = wr.WriteString("\r\n")
}

// writeMultiBulk writes a resp array
func writeMultiBulk(wr *bytes.Buffer, bulks ...string) {
	writeHead(wr, '*', len(bulks))
	for _, bulk := range bulks {
		writeHead(wr, '$', len(bulk))
		_, _ = wr.WriteString(bulk)
		_, _ = wr.WriteString("\r\n")
	}
}

// writeSetTo writes an item as a single SET record to the a bufio Writer.
func (dbi *dbItem) writeSetTo(wr *bytes.Buffer) {
	if dbi.opts != nil && dbi.opts.ex {
		ex := strconv.FormatUint(
			uint64(dbi.opts.exat.Sub(time.Now())/time.Second),
			10,
		)
		writeMultiBulk(wr, "set", dbi.key, dbi.val, "ex", ex)
	} else {
		writeMultiBulk(wr, "set", dbi.key, dbi.val)
	}
}

// writeSetTo writes an item as a single DEL record to the a bufio Writer.
func (dbi *dbItem) writeDeleteTo(wr *bytes.Buffer) {
	writeMultiBulk(wr, "del", dbi.key)
}

// expired evaluates id the item has expired. This will always return false when
// the item does not have `opts.ex` set to true.
func (dbi *dbItem) expired() bool {
	return dbi.opts != nil && dbi.opts.ex && time.Now().After(dbi.opts.exat)
}

// MaxTime from http://stackoverflow.com/questions/25065055#32620397
// This is a long time in the future. It's an imaginary number that is
// used for b-tree ordering.
var maxTime = time.Unix(1<<63-62135596801, 999999999)

// expiresAt will return the time when the item will expire. When an item does
// not expire `maxTime` is used.
func (dbi *dbItem) expiresAt() time.Time {
	if dbi.opts == nil || !dbi.opts.ex {
		return maxTime
	}
	return dbi.opts.exat
}

// Less determines if a b-tree item is less than another. This is required
// for ordering, inserting, and deleting items from a b-tree. It's important
// to note that the ctx parameter is used to help with determine which
// formula to use on an item. Each b-tree should use a different ctx when
// sharing the same item.
func (dbi *dbItem) Less(item btree.Item, ctx interface{}) bool {
	dbi2 := item.(*dbItem)
	switch ctx := ctx.(type) {
	case *exctx:
		// The expires b-tree formula
		if dbi2.expiresAt().After(dbi.expiresAt()) {
			return true
		}
		if dbi.expiresAt().After(dbi2.expiresAt()) {
			return false
		}
	case *index:
		if ctx.less != nil {
			// Using an index
			if ctx.less(dbi.val, dbi2.val) {
				return true
			}
			if ctx.less(dbi2.val, dbi.val) {
				return false
			}
		}
	}
	// Always fall back to the key comparison. This creates absolute uniqueness.
	return dbi.key < dbi2.key
}

// Rect converts a string to a rectangle.
// An invalid rectangle will cause a panic.
func (dbi *dbItem) Rect(ctx interface{}) (min, max []float64) {
	switch ctx := ctx.(type) {
	case *index:
		return ctx.rect(dbi.val)
	}
	return nil, nil
}

// SetOptions represents options that may be included with the Set() command.
type SetOptions struct {
	// Expires indicates that the Set() key-value will expire
	Expires bool
	// TTL is how much time the key-value will exist in the database
	// before being evicted. The Expires field must also be set to true.
	// TTL stands for Time-To-Live.
	TTL time.Duration
}

// Set inserts or replaces an item in the database based on the key.
// The opt params may be used for additional functionality such as forcing
// the item to be evicted at a specified time. When the return value
// for err is nil the operation succeeded. When the return value of
// replaced is true, then the operaton replaced an existing item whose
// value will be returned through the previousValue variable.
// The results of this operation will not be available to other
// transactions until the current transaction has successfully committed.
func (tx *Tx) Set(key, value string, opts *SetOptions) (previousValue string,
	replaced bool, err error) {
	if tx.db == nil {
		return "", false, ErrTxClosed
	} else if !tx.writable {
		return "", false, ErrTxNotWritable
	}
	item := &dbItem{key: key, val: value}
	if opts != nil {
		if opts.Expires {
			// The caller is requesting that this item expires. Convert the
			// TTL to an absolute time and bind it to the item.
			item.opts = &dbItemOpts{ex: true, exat: time.Now().Add(opts.TTL)}
		}
	}
	// Insert the item into the keys tree.
	prev := tx.db.insertIntoDatabase(item)
	if prev == nil {
		// An item with the same key did not previously exist. Let's create a
		// rollback entry with a nil value. A nil value indicates that the
		// entry should be deleted on rollback. When the value is *not* nil,
		// that means the entry should be reverted.
		tx.rollbacks[key] = nil
	} else {
		// A previous item already exists in the database. Let's create a
		// rollback entry with the item as the value. We need to check the map
		// to see if there isn't already an item that matches the same key.
		if _, ok := tx.rollbacks[key]; !ok {
			tx.rollbacks[key] = prev
		}
		if !prev.expired() {
			previousValue, replaced = prev.val, true
		}
	}
	// For commits we simply assign the item to the map. We use this map to
	// write the entry to disk.
	if tx.db.persist {
		tx.commits[key] = item
	}
	return previousValue, replaced, nil
}

// Get returns a value for a key. If the item does not exist or if the item
// has expired then ErrNotFound is returned.
func (tx *Tx) Get(key string) (val string, err error) {
	if tx.db == nil {
		return "", ErrTxClosed
	}
	item := tx.db.get(key)
	if item == nil || item.expired() {
		// The item does not exists or has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.val, nil
}

// Delete removes an item from the database based on the item's key. If the item
// does not exist or if the item has expired then ErrNotFound is returned.
//
// Only writable transaction can be used for Delete() calls.
func (tx *Tx) Delete(key string) (val string, err error) {
	if tx.db == nil {
		return "", ErrTxClosed
	} else if !tx.writable {
		return "", ErrTxNotWritable
	}
	item := tx.db.deleteFromDatabase(&dbItem{key: key})
	if item == nil {
		return "", ErrNotFound
	}
	if _, ok := tx.rollbacks[key]; !ok {
		tx.rollbacks[key] = item
	}
	if tx.db.persist {
		tx.commits[key] = nil
	}
	// Even though the item has been deleted, we still want to check
	// if it has expired. An expired item should not be returned.
	if item.expired() {
		// The item exists in the tree, but has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		return "", ErrNotFound
	}
	return item.val, nil
}

// TTL returns the remaining time-to-live for an item.
// A negative duration will be returned for items that do not have an
// expiration.
func (tx *Tx) TTL(key string) (time.Duration, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	item := tx.db.get(key)
	if item == nil {
		return 0, ErrNotFound
	} else if item.opts == nil || !item.opts.ex {
		return -1, nil
	}
	dur := item.opts.exat.Sub(time.Now())
	if dur < 0 {
		return 0, ErrNotFound
	}
	return dur, nil
}

// scan iterates through a specified index and calls user-defined iterator
// function for each item encountered.
// The desc param indicates that the iterator should descend.
// The gt param indicates that there is a greaterThan limit.
// The lt param indicates that there is a lessThan limit.
// The index param tells the scanner to use the specified index tree. An
// empty string for the index means to scan the keys, not the values.
// The start and stop params are the greaterThan, lessThan limits. For
// descending order, these will be lessThan, greaterThan.
// An error will be returned if the tx is closed or the index is not found.
func (tx *Tx) scan(desc, gt, lt bool, index, start, stop string,
	iterator func(key, value string) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	// wrap a btree specific iterator around the user-defined iterator.
	iter := func(item btree.Item) bool {
		dbi := item.(*dbItem)
		return iterator(dbi.key, dbi.val)
	}
	var tr *btree.BTree
	if index == "" {
		// empty index means we will use the keys tree.
		tr = tx.db.keys
	} else {
		idx := tx.db.idxs[index]
		if idx == nil {
			// index was not found. return error
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	var itemA, itemB *dbItem
	if gt || lt {
		if index == "" {
			itemA = &dbItem{key: start}
			itemB = &dbItem{key: stop}
		} else {
			itemA = &dbItem{val: start}
			itemB = &dbItem{val: stop}
		}
	}
	// execute the scan on the underlying tree.
	if desc {
		if gt {
			if lt {
				tr.DescendRange(itemA, itemB, iter)
			} else {
				tr.DescendGreaterThan(itemA, iter)
			}
		} else if lt {
			tr.DescendLessOrEqual(itemA, iter)
		} else {
			tr.Descend(iter)
		}
	} else {
		if gt {
			if lt {
				tr.AscendRange(itemA, itemB, iter)
			} else {
				tr.AscendGreaterOrEqual(itemA, iter)
			}
		} else if lt {
			tr.AscendLessThan(itemA, iter)
		} else {
			tr.Ascend(iter)
		}
	}
	return nil
}

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Ascend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, false, index, "", "", iterator)
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendGreaterOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, true, false, index, pivot, "", iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendLessThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, true, index, pivot, "", iterator)
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendRange(index, greaterOrEqual, lessThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		false, true, true, index, greaterOrEqual, lessThan, iterator,
	)
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Descend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, false, index, "", "", iterator)
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendGreaterThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, true, false, index, pivot, "", iterator)
}

// DescendLessOrEqual calls the iterator for every item in the database within
// the range [pivot, first], until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendLessOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, true, index, pivot, "", iterator)
}

// DescendRange calls the iterator for every item in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// When an index is provided, the results will be ordered by the item values
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendRange(index, lessOrEqual, greaterThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}

// rect is used by Intersects
type rect struct {
	min, max []float64
}

func (r *rect) Rect(ctx interface{}) (min, max []float64) {
	return r.min, r.max
}

// Intersects searches for rectangle items that intersect a target rect.
// The specified index must have been created by AddIndex() and the target
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid index will return an error.
func (tx *Tx) Intersects(index, bounds string,
	iterator func(key, value string) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	if index == "" {
		// cannot search on keys tree. just return nil.
		return nil
	}
	// wrap a rtree specific iterator around the user-defined iterator.
	iter := func(item rtree.Item) bool {
		dbi := item.(*dbItem)
		return iterator(dbi.key, dbi.val)
	}
	idx := tx.db.idxs[index]
	if idx == nil {
		// index was not found. return error
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree index. just return nil
		return nil
	}
	// execute the search
	var min, max []float64
	if idx.rect != nil {
		min, max = idx.rect(bounds)
	}
	idx.rtr.Search(&rect{min, max}, iter)
	return nil
}

// Len returns the number of items in the database
func (tx *Tx) Len() (int, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	return tx.db.keys.Len(), nil
}

// Rect is helper function that returns a string representation
// of a rect. IndexRect() is the reverse function and can be used
// to generate a rect from a string.
func Rect(min, max []float64) string {
	if min == nil && max == nil {
		return ""
	}
	diff := len(min) != len(max)
	if !diff {
		for i := 0; i < len(min); i++ {
			if min[i] != max[i] {
				diff = true
				break
			}
		}
	}
	var b bytes.Buffer
	_ = b.WriteByte('[')
	for i, v := range min {
		if i > 0 {
			_ = b.WriteByte(' ')
		}
		_, _ = b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
	}
	if diff {
		_, _ = b.WriteString("],[")
		for i, v := range max {
			if i > 0 {
				_ = b.WriteByte(' ')
			}
			_, _ = b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
		}
	}
	_ = b.WriteByte(']')
	return b.String()
}

// Point is a helper function that converts a series of float64s
// to a rectangle for a spatial index.
func Point(coords ...float64) string {
	return Rect(coords, coords)
}

// IndexRect is a helper function that converts string to a rect.
// Rect() is the reverse function and can be used to generate a string
// from a rect.
func IndexRect(a string) (min, max []float64) {
	parts := strings.Split(a, ",")
	for i := 0; i < len(parts) && i < 2; i++ {
		part := parts[i]
		if len(part) >= 2 && part[0] == '[' && part[len(part)-1] == ']' {
			pieces := strings.Split(part[1:len(part)-1], " ")
			if i == 0 {
				min = make([]float64, 0, len(pieces))
			} else {
				max = make([]float64, 0, len(pieces))
			}
			for j := 0; j < len(pieces); j++ {
				piece := pieces[j]
				if piece != "" {
					n, _ := strconv.ParseFloat(piece, 64)
					if i == 0 {
						min = append(min, n)
					} else {
						max = append(max, n)
					}
				}
			}
		}
	}
	if len(parts) == 1 {
		max = min
	}
	return
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// case-sensitive strings.
func IndexString(a, b string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] >= 'A' && a[i] <= 'Z' {
			if b[i] >= 'A' && b[i] <= 'Z' {
				// both are uppercase, do nothing
				if a[i] < b[i] {
					return true
				} else if a[i] > b[i] {
					return false
				}
			} else {
				// a is uppercase, convert a to lowercase
				if a[i]+32 < b[i] {
					return true
				} else if a[i]+32 > b[i] {
					return false
				}
			}
		} else if b[i] >= 'A' && b[i] <= 'Z' {
			// b is uppercase, convert b to lowercase
			if a[i] < b[i]+32 {
				return true
			} else if a[i] > b[i]+32 {
				return false
			}
		} else {
			// neither are uppercase
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
	}
	return len(a) < len(b)
}

// IndexBinary is a helper function that returns true if 'a' is less than 'b'.
// This compares the raw binary of the string.
func IndexBinary(a, b string) bool {
	return a < b
}

// IndexInt is a helper function that returns true if 'a' is less than 'b'.
func IndexInt(a, b string) bool {
	ia, _ := strconv.ParseInt(a, 10, 64)
	ib, _ := strconv.ParseInt(b, 10, 64)
	return ia < ib
}

// IndexUint is a helper function that returns true if 'a' is less than 'b'.
// This compares uint64s that are added to the database using the
// Uint() conversion function.
func IndexUint(a, b string) bool {
	ia, _ := strconv.ParseUint(a, 10, 64)
	ib, _ := strconv.ParseUint(b, 10, 64)
	return ia < ib
}

// IndexFloat is a helper function that returns true if 'a' is less than 'b'.
// This compares float64s that are added to the database using the
// Float() conversion function.
func IndexFloat(a, b string) bool {
	ia, _ := strconv.ParseFloat(a, 64)
	ib, _ := strconv.ParseFloat(b, 64)
	return ia < ib
}

// IndexJSON provides for the ability to create an index on any JSON field.
// When the field is a string, the comparison will be case-insensitive.
// It returns a helper function used by CreateIndex.
func IndexJSON(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), false)
	}
}

// IndexJSONCaseSensitive provides for the ability to create an index on
// any JSON field.
// When the field is a string, the comparison will be case-sensitive.
// It returns a helper function used by CreateIndex.
func IndexJSONCaseSensitive(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), true)
	}
}

// Desc is a helper function that changes the order of an index.
func Desc(less func(a, b string) bool) func(a, b string) bool {
	return func(a, b string) bool { return less(b, a) }
}
