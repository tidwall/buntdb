package buntdb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBackgroudOperations(t *testing.T) {
	os.RemoveAll("data.db")
	db, err := Open("data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("data.db")
	defer db.Close()
	for i := 0; i < 1000; i++ {
		if err := db.Update(func(tx *Tx) error {
			for j := 0; j < 200; j++ {
				tx.Set(fmt.Sprintf("hello%d", j), "planet", nil)
			}
			tx.Set("hi", "world", &SetOptions{Expires: true, TTL: time.Second / 2})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	db.View(func(tx *Tx) error {
		n, _ = tx.Len()
		return nil
	})
	if n != 201 {
		t.Fatalf("expecting '%v', got '%v'", 201, n)
	}
	time.Sleep(time.Millisecond * 1500)
	db.Close()
	db, err = Open("data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("data.db")
	defer db.Close()
	n = 0
	db.View(func(tx *Tx) error {
		n, _ = tx.Len()
		return nil
	})
	if n != 200 {
		t.Fatalf("expecting '%v', got '%v'", 200, n)
	}
}
func TestVariousTx(t *testing.T) {
	os.RemoveAll("data.db")
	db, err := Open("data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("data.db")
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		tx.Set("hello", "planet", nil)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	errBroken := errors.New("broken")
	if err := db.Update(func(tx *Tx) error {
		tx.Set("hello", "world", nil)
		return errBroken
	}); err != errBroken {
		t.Fatalf("did not correctly receive the user-defined transaction error.")
	}
	var val string
	db.View(func(tx *Tx) error {
		val, _ = tx.Get("hello")
		return nil
	})
	if val == "world" {
		t.Fatal("a rollbacked transaction got through")
	}
	if val != "planet" {
		t.Fatalf("expecting '%v', got '%v'", "planet", val)
	}
	if err := db.Update(func(tx *Tx) error {
		tx.db = nil
		if _, _, err := tx.Set("hello", "planet", nil); err != ErrTxClosed {
			t.Fatal("expecting a tx closed error")
		}
		if _, err := tx.Delete("hello"); err != ErrTxClosed {
			t.Fatal("expecting a tx closed error")
		}
		if _, err := tx.Get("hello"); err != ErrTxClosed {
			t.Fatal("expecting a tx closed error")
		}
		tx.db = db
		tx.writable = false
		if _, _, err := tx.Set("hello", "planet", nil); err != ErrTxNotWritable {
			t.Fatal("expecting a tx not writable error")
		}
		if _, err := tx.Delete("hello"); err != ErrTxNotWritable {
			t.Fatal("expecting a tx not writable error")
		}
		tx.writable = true
		if _, err := tx.Get("something"); err != ErrNotFound {
			t.Fatalf("expecting not found error")
		}
		if _, err := tx.Delete("something"); err != ErrNotFound {
			t.Fatalf("expecting not found error")
		}
		tx.Set("var", "val", &SetOptions{Expires: true, TTL: 0})
		if _, err := tx.Get("var"); err != ErrNotFound {
			t.Fatalf("expecting not found error")
		}
		if _, err := tx.Delete("var"); err != ErrNotFound {
			tx.unlock()
			t.Fatalf("expecting not found error")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// test for invalid commits
	if err := db.Update(func(tx *Tx) error {
		// we are going to do some hackery
		defer func() {
			if v := recover(); v != nil {
				if v.(string) != "managed tx commit not allowed" {
					t.Fatal(v.(string))
				}
			}
		}()
		tx.commit()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// test for invalid commits
	if err := db.Update(func(tx *Tx) error {
		// we are going to do some hackery
		defer func() {
			if v := recover(); v != nil {
				if v.(string) != "managed tx rollback not allowed" {
					t.Fatal(v.(string))
				}
			}
		}()
		tx.rollback()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// test for closed transactions
	if err := db.Update(func(tx *Tx) error {
		tx.db = nil
		return nil
	}); err != ErrTxClosed {
		t.Fatal("expecting tx closed error")
	}
	db.mu.Unlock()

	// test for invalid writes
	if err := db.Update(func(tx *Tx) error {
		tx.writable = false
		return nil
	}); err != ErrTxNotWritable {
		t.Fatal("expecting tx not writable error")
	}
	db.mu.Unlock()
	// test for closed transactions
	if err := db.View(func(tx *Tx) error {
		tx.db = nil
		return nil
	}); err != ErrTxClosed {
		t.Fatal("expecting tx closed error")
	}
	db.mu.RUnlock()
	// flush to unwritable file
	if err := db.Update(func(tx *Tx) error {
		tx.Set("var1", "val1", nil)
		tx.db.file.Close()
		return nil
	}); err == nil {
		t.Fatal("should not be able to commit when the file is closed")
	}
	db.file, err = os.OpenFile("data.db", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.file.Seek(0, 2); err != nil {
		t.Fatal(err)
	}
	db.bufw = bufio.NewWriter(db.file)
	db.CreateIndex("blank", "*", nil)
	// test scanning
	if err := db.Update(func(tx *Tx) error {
		tx.Set("nothing", "here", nil)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		s := ""
		tx.Ascend("", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if s != "hello:planet\nnothing:here\n" {
			t.Fatal("invalid scan")
		}
		tx.db = nil
		err = tx.Ascend("", func(key, val string) bool { return true })
		if err != ErrTxClosed {
			tx.unlock()
			t.Fatal("expecting tx closed error")
		}
		tx.db = db
		err = tx.Ascend("na", func(key, val string) bool { return true })
		if err != ErrNotFound {
			t.Fatal("expecting not found error")
		}
		err = tx.Ascend("blank", func(key, val string) bool { return true })
		if err != nil {
			t.Fatal(err)
		}
		s = ""
		tx.AscendLessThan("", "liger", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if s != "hello:planet\n" {
			t.Fatal("invalid scan")
		}

		s = ""
		tx.Descend("", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if s != "nothing:here\nhello:planet\n" {
			t.Fatal("invalid scan")
		}

		s = ""
		tx.DescendLessOrEqual("", "liger", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if s != "hello:planet\n" {
			t.Fatal("invalid scan")
		}

		s = ""
		tx.DescendGreaterThan("", "liger", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if s != "nothing:here\n" {
			t.Fatal("invalid scan")
		}
		s = ""
		tx.DescendRange("", "liger", "apple", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if s != "hello:planet\n" {
			t.Fatal("invalid scan")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// test some spatial stuff
	db.CreateSpatialIndex("spat", "rect:*", IndexRect)
	db.CreateSpatialIndex("junk", "rect:*", nil)
	db.Update(func(tx *Tx) error {
		tx.Set("rect:1", "[10 10],[20 20]", nil)
		tx.Set("rect:2", "[15 15],[25 25]", nil)
		tx.Set("shape:1", "[12 12],[25 25]", nil)
		s := ""
		tx.Intersects("spat", "[5 5],[13 13]", func(key, val string) bool {
			s += key + ":" + val + "\n"
			return true
		})
		if s != "rect:1:[10 10],[20 20]\n" {
			t.Fatal("invalid scan")
		}
		tx.db = nil
		err := tx.Intersects("spat", "[5 5],[13 13]", func(key, val string) bool {
			return true
		})
		if err != ErrTxClosed {
			t.Fatal("expecting tx closed error")
		}
		tx.db = db
		err = tx.Intersects("", "[5 5],[13 13]", func(key, val string) bool {
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.Intersects("na", "[5 5],[13 13]", func(key, val string) bool {
			return true
		})
		if err != ErrNotFound {
			t.Fatal("expecting not found error")
		}
		err = tx.Intersects("junk", "[5 5],[13 13]", func(key, val string) bool {
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		n, err := tx.Len()
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expecting %v, got %v", 5, n)
		}
		tx.db = nil
		_, err = tx.Len()
		if err != ErrTxClosed {
			t.Fatal("expecting tx closed error")
		}
		tx.db = db
		return nil
	})

	// test after closing
	db.Close()
	if err := db.Update(func(tx *Tx) error { return nil }); err != ErrDatabaseClosed {
		t.Fatalf("should not be able to perform transactionso on a closed database.")
	}
}
func TestNoExpiringItem(t *testing.T) {
	item := &dbItem{key: "key", val: "val"}
	if !item.expiresAt().Equal(maxTime) {
		t.Fatal("item.expiresAt() != maxTime")
	}
	if min, max := item.Rect(nil); min != nil || max != nil {
		t.Fatal("item min,max should both be nil")
	}
}

// test database format loading
func TestDatabaseFormat(t *testing.T) {
	// should succeed
	func() {
		resp := strings.Join([]string{
			"*3\r\n$3\r\nset\r\n$4\r\nvar1\r\n$4\r\n1234\r\n",
			"*3\r\n$3\r\nset\r\n$4\r\nvar2\r\n$4\r\n1234\r\n",
			"*2\r\n$3\r\ndel\r\n$4\r\nvar1\r\n",
			"*5\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\n10\r\n",
		}, "")
		os.RemoveAll("data.db")
		ioutil.WriteFile("data.db", []byte(resp), 0666)
		db, err := Open("data.db")
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll("data.db")
		defer db.Close()
	}()
	testBadFormat := func(resp string) {
		os.RemoveAll("data.db")
		ioutil.WriteFile("data.db", []byte(resp), 0666)
		db, err := Open("data.db")
		if err == nil {
			db.Close()
			os.RemoveAll("data.db")
			t.Fatalf("invalid database should not be allowed")
		}
	}
	testBadFormat("*3\r")
	testBadFormat("*3\n")
	testBadFormat("*a\r\n")
	testBadFormat("*2\r\n")
	testBadFormat("*2\r\n%3")
	testBadFormat("*2\r\n$")
	testBadFormat("*2\r\n$3\r\n")
	testBadFormat("*2\r\n$3\r\ndel")
	testBadFormat("*2\r\n$3\r\ndel\r\r")
	testBadFormat("*0\r\n*2\r\n$3\r\ndel\r\r")
	testBadFormat("*1\r\n$3\r\nnop\r\n")
	testBadFormat("*1\r\n$3\r\ndel\r\n")
	testBadFormat("*1\r\n$3\r\nset\r\n")
	testBadFormat("*5\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nxx\r\n$2\r\n10\r\n")
	testBadFormat("*5\r\n$3\r\nset\r\n$3\r\nvar\r\n$3\r\nval\r\n$2\r\nex\r\n$2\r\naa\r\n")
}

func TestInsertsAndDeleted(t *testing.T) {
	os.RemoveAll("data.db")
	db, err := Open("data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("data.db")
	defer db.Close()
	db.CreateIndex("any", "*", IndexString)
	db.CreateSpatialIndex("rect", "*", IndexRect)
	if err := db.Update(func(tx *Tx) error {
		tx.Set("item1", "value1", &SetOptions{Expires: true, TTL: time.Second})
		tx.Set("item2", "value2", nil)
		tx.Set("item3", "value3", &SetOptions{Expires: true, TTL: time.Second})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// test replacing items in the database
	if err := db.Update(func(tx *Tx) error {
		if _, _, err := tx.Set("item1", "nvalue1", nil); err != nil {
			return err
		}
		if _, _, err := tx.Set("item2", "nvalue2", nil); err != nil {
			return err
		}
		if _, err := tx.Delete("item3"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// test index compare functions
func TestIndexCompare(t *testing.T) {
	if !IndexFloat("1.5", "1.6") {
		t.Fatalf("expected true, got false")
	}
	if !IndexInt("-1", "2") {
		t.Fatalf("expected true, got false")
	}
	if !IndexUint("10", "25") {
		t.Fatalf("expected true, got false")
	}
	if !IndexBinary("Hello", "hello") {
		t.Fatalf("expected true, got false")
	}
	if IndexString("hello", "hello") {
		t.Fatalf("expected false, got true")
	}
	if IndexString("Hello", "hello") {
		t.Fatalf("expected false, got true")
	}
	if IndexString("hello", "Hello") {
		t.Fatalf("expected false, got true")
	}
	if !IndexString("gello", "Hello") {
		t.Fatalf("expected true, got false")
	}
	if IndexString("Hello", "gello") {
		t.Fatalf("expected false, got true")
	}
	if Rect(IndexRect("[1 2 3 4],[5 6 7 8]")) != "[1 2 3 4],[5 6 7 8]" {
		t.Fatalf("expected '%v', got '%v'", "[1 2 3 4],[5 6 7 8]", Rect(IndexRect("[1 2 3 4],[5 6 7 8]")))
	}
	if Rect(IndexRect("[1 2 3 4]")) != "[1 2 3 4]" {
		t.Fatalf("expected '%v', got '%v'", "[1 2 3 4]", Rect(IndexRect("[1 2 3 4]")))
	}
	if Rect(nil, nil) != "" {
		t.Fatalf("expected '%v', got '%v'", "", Rect(nil, nil))
	}
	if Point(1, 2, 3) != "[1 2 3]" {
		t.Fatalf("expected '%v', got '%v'", "[1 2 3]", Point(1, 2, 3))
	}
}

// test opening a folder.
func TestOpeningAFolder(t *testing.T) {
	os.RemoveAll("dir.tmp")
	os.Mkdir("dir.tmp", 0700)
	defer os.RemoveAll("dir.tmp")
	db, err := Open("dir.tmp")
	if err == nil {
		db.Close()
		t.Fatalf("opening a directory should not be allowed")
	}
}

// test opening an invalid resp file.
func TestOpeningInvalidDatabaseFile(t *testing.T) {
	os.RemoveAll("data.db")
	ioutil.WriteFile("data.db", []byte("invalid\r\nfile"), 0666)
	defer os.RemoveAll("data.db")
	db, err := Open("data.db")
	if err == nil {
		db.Close()
		t.Fatalf("invalid database should not be allowed")
	}
}

// test closing a closed database.
func TestOpeningClosedDatabase(t *testing.T) {
	os.RemoveAll("data.db")
	db, err := Open("data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("data.db")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != ErrDatabaseClosed {
		t.Fatal("should not be able to close a closed database")
	}
}

func TestVariousIndexOperations(t *testing.T) {
	os.RemoveAll("data.db")
	db, err := Open("data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("data.db")
	defer db.Close()
	// test creating an index with no index name.
	err = db.CreateIndex("", "", nil)
	if err == nil {
		t.Fatal("should not be able to create an index with no name")
	}
	// test creating an index with a name that has already been used.
	err = db.CreateIndex("hello", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.CreateIndex("hello", "", nil)
	if err == nil {
		t.Fatal("should not be able to create a duplicate index")
	}
	db.Update(func(tx *Tx) error {
		tx.Set("user:1", "tom", nil)
		tx.Set("user:2", "janet", nil)
		tx.Set("alt:1", "from", nil)
		tx.Set("alt:2", "there", nil)
		tx.Set("rect:1", "[1 2],[3 4]", nil)
		tx.Set("rect:2", "[5 6],[7 8]", nil)
		return nil
	})
	// test creating an index after adding items. use pattern matching. have some items in the match and some not.
	if err := db.CreateIndex("string", "user:*", IndexString); err != nil {
		t.Fatal(err)
	}
	// test creating a spatial index after adding items. use pattern matching. have some items in the match and some not.
	if err := db.CreateSpatialIndex("rect", "rect:*", IndexRect); err != nil {
		t.Fatal(err)
	}
	// test dropping an index
	if err := db.DropIndex("hello"); err != nil {
		t.Fatal(err)
	}
	// test dropping an index with no name
	if err := db.DropIndex(""); err == nil {
		t.Fatal("should not be allowed to drop an index with no name")
	}
	// test dropping an index with no name
	if err := db.DropIndex("na"); err == nil {
		t.Fatal("should not be allowed to drop an index that does not exist")
	}
	// test retrieving index names
	names, err := db.Indexes()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(names, ",") != "rect,string" {
		t.Fatalf("expecting '%v', got '%v'", "rect,string", strings.Join(names, ","))
	}
	// test creating an index after closing database
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("new-index", "", nil); err != ErrDatabaseClosed {
		t.Fatal("should not be able to create an index on a closed database")
	}
	// test getting index names after closing database
	if _, err := db.Indexes(); err != ErrDatabaseClosed {
		t.Fatal("should not be able to get index names on a closed database")
	}
	// test dropping an index after closing database
	if err := db.DropIndex("rect"); err != ErrDatabaseClosed {
		t.Fatal("should not be able to drop an index on a closed database")
	}
}

func test(t *testing.T, a, b bool) {
	if a != b {
		t.Fatal("failed, bummer...")
	}
}

func TestPatternMatching(t *testing.T) {
	test(t, wildcardMatch("hello", "hello"), true)
	test(t, wildcardMatch("hello", "h*"), true)
	test(t, wildcardMatch("hello", "h*o"), true)
	test(t, wildcardMatch("hello", "h*l*o"), true)
	test(t, wildcardMatch("hello", "h*z*o"), false)
	test(t, wildcardMatch("hello", "*l*o"), true)
	test(t, wildcardMatch("hello", "*l*"), true)
	test(t, wildcardMatch("hello", "*?*"), true)
	test(t, wildcardMatch("hello", "*"), true)
	test(t, wildcardMatch("hello", "h?llo"), true)
	test(t, wildcardMatch("hello", "h?l?o"), true)
	test(t, wildcardMatch("", "*"), true)
	test(t, wildcardMatch("", ""), true)
	test(t, wildcardMatch("h", ""), false)
	test(t, wildcardMatch("", "?"), false)
}

func TestBasic(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	os.RemoveAll("data.db")
	db, err := Open("data.db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("data.db")
	defer db.Close()

	// create a simple index
	db.CreateIndex("users", "fun:user:*", IndexString)

	// create a spatial index
	db.CreateSpatialIndex("rects", "rect:*", IndexRect)
	if true {
		db.Update(func(tx *Tx) error {
			tx.Set("fun:user:0", "tom", nil)
			tx.Set("fun:user:1", "Randi", nil)
			tx.Set("fun:user:2", "jane", nil)
			tx.Set("fun:user:4", "Janet", nil)
			tx.Set("fun:user:5", "Paula", nil)
			tx.Set("fun:user:6", "peter", nil)
			tx.Set("fun:user:7", "Terri", nil)
			return nil
		})
		// add some random items
		start := time.Now()
		if err := db.Update(func(tx *Tx) error {
			for _, i := range rand.Perm(100) {
				tx.Set(fmt.Sprintf("tag:%d", i+100), fmt.Sprintf("val:%d", rand.Int()%100+100), nil)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if false {
			println(time.Now().Sub(start).String(), db.keys.Len())
		}
		// add some random rects
		if err := db.Update(func(tx *Tx) error {
			tx.Set("rect:1", Rect([]float64{10, 10}, []float64{20, 20}), nil)
			tx.Set("rect:2", Rect([]float64{15, 15}, []float64{24, 24}), nil)
			tx.Set("rect:3", Rect([]float64{17, 17}, []float64{27, 27}), nil)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	// verify the data has been created
	buf := &bytes.Buffer{}
	db.View(func(tx *Tx) error {
		tx.Ascend("users", func(key, val string) bool {
			fmt.Fprintf(buf, "%s %s\n", key, val)
			return true
		})
		err = tx.AscendRange("", "tag:170", "tag:172", func(key, val string) bool {
			fmt.Fprintf(buf, "%s\n", key)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.AscendGreaterOrEqual("", "tag:195", func(key, val string) bool {
			fmt.Fprintf(buf, "%s\n", key)
			return true
		})
		if err != nil {
			t.Fatal(err)
		}
		err = tx.AscendGreaterOrEqual("", "rect:", func(key, val string) bool {
			if !strings.HasPrefix(key, "rect:") {
				return false
			}
			min, max := IndexRect(val)
			fmt.Fprintf(buf, "%s: %v,%v\n", key, min, max)
			return true
		})
		expect := make([]string, 2)
		n := 0
		tx.Intersects("rects", "[0 0],[15 15]", func(key, val string) bool {
			if n == 2 {
				t.Fatalf("too many rects where received, expecting only two")
			}
			min, max := IndexRect(val)
			s := fmt.Sprintf("%s: %v,%v\n", key, min, max)
			if key == "rect:1" {
				expect[0] = s
			} else if key == "rect:2" {
				expect[1] = s
			}
			n++
			return true
		})
		for _, s := range expect {
			buf.WriteString(s)
		}
		return nil
	})
	res := `
fun:user:2 jane
fun:user:4 Janet
fun:user:5 Paula
fun:user:6 peter
fun:user:1 Randi
fun:user:7 Terri
fun:user:0 tom
tag:170
tag:171
tag:195
tag:196
tag:197
tag:198
tag:199
rect:1: [10 10],[20 20]
rect:2: [15 15],[24 24]
rect:3: [17 17],[27 27]
rect:1: [10 10],[20 20]
rect:2: [15 15],[24 24]
`
	res = strings.Replace(res, "\r", "", -1)
	if strings.TrimSpace(buf.String()) != strings.TrimSpace(res) {
		t.Fatalf("expected [%v], got [%v]", strings.TrimSpace(res), strings.TrimSpace(buf.String()))
	}
}
func testRectStringer(min, max []float64) error {
	nmin, nmax := IndexRect(Rect(min, max))
	if len(nmin) != len(min) {
		return fmt.Errorf("rect=%v,%v, expect=%v,%v", nmin, nmax, min, max)
	}
	for i := 0; i < len(min); i++ {
		if min[i] != nmin[i] || max[i] != nmax[i] {
			return fmt.Errorf("rect=%v,%v, expect=%v,%v", nmin, nmax, min, max)
		}
	}
	return nil
}
func TestRectStrings(t *testing.T) {
	test(t, Rect(IndexRect(Point(1))) == "[1]", true)
	test(t, Rect(IndexRect(Point(1, 2, 3, 4))) == "[1 2 3 4]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1 2],[1 2]")))) == "[1 2]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1 2],[2 2]")))) == "[1 2],[2 2]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1 2],[2 2],[3]")))) == "[1 2],[2 2]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1 2]")))) == "[1 2]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1.5 2 4.5 5.6]")))) == "[1.5 2 4.5 5.6]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[1.5 2 4.5 5.6 -1],[]")))) == "[1.5 2 4.5 5.6 -1],[]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("[]")))) == "[]", true)
	test(t, Rect(IndexRect(Rect(IndexRect("")))) == "", true)
	if err := testRectStringer(nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{}, []float64{}); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{1}, []float64{2}); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{1, 2}, []float64{3, 4}); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{1, 2, 3}, []float64{4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{1, 2, 3, 4}, []float64{5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	if err := testRectStringer([]float64{1, 2, 3, 4, 5}, []float64{6, 7, 8, 9, 0}); err != nil {
		t.Fatal(err)
	}
}
