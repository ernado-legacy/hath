package hath

import (
	"errors"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
)

const (
	dbDir      = "db"
	dbFileMode = 0600
	dbBulkSize = 10000
)

var (
	dbFileBucket = []byte("files")
	dbOptions    = bolt.Options{Timeout: 1 * time.Second}
)

// DataBase is interface for storing info about files in some DB
type DataBase interface {
}

// BoltDB stores info about files in cache
type BoltDB struct {
	db *bolt.DB
}

// NewDB new db
func NewDB(dbPath string) (d *BoltDB, err error) {
	d = new(BoltDB)

	db, err := bolt.Open(dbPath, 0600, &dbOptions)
	if err != nil {
		return
	}

	tx, err := db.Begin(true)
	if err != nil {
		return
	}
	defer tx.Rollback()
	_, err = tx.CreateBucketIfNotExists(dbFileBucket)
	if err != nil {
		return
	}
	if err = tx.Commit(); err != nil {
		return
	}
	d.db = db

	return d, nil
}

func (d BoltDB) add(f File) error {
	data, err := f.Marshal()
	if err != nil {
		return err
	}
	tx, err := d.db.Begin(true)
	defer tx.Rollback()
	if err != nil {
		return err
	}
	if err := tx.Bucket(dbFileBucket).Put(f.ByteID(), data); err != nil {
		return err
	}
	return tx.Commit()
}

// Close closes boltdb internal database
func (d BoltDB) Close() error {
	return d.db.Close()
}

// Add inserts file info to db
func (d BoltDB) Add(f File) error {
	return d.add(f)
}

// AddBatch inserts slice of files into db
func (d BoltDB) AddBatch(files []File) error {
	count := len(files)
	if count > dbBulkSize {
		var i int
		for i = 0; i+dbBulkSize < count; i += dbBulkSize {
			if err := d.AddBatch(files[i : i+dbBulkSize]); err != nil {
				log.Fatal(err)
			}
		}
		if err := d.AddBatch(files[i:]); err != nil {
			return err
		}
		return nil
	}
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bucket := tx.Bucket(dbFileBucket)
	for _, f := range files {
		data, err := f.Marshal()
		if err != nil {
			return err
		}
		if err := bucket.Put(f.ByteID(), data); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Collect removes files that LastUsage is after deadline
func (d BoltDB) Collect(deadline time.Time) (int, error) {
	tx, err := d.db.Begin(true)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var markedFiles [][]byte
	var f File
	err = tx.Bucket(dbFileBucket).ForEach(func(k []byte, v []byte) error {
		if err := UnmarshalFileTo(v, &f); err != nil {
			return err
		}
		if f.LastUsageBefore(deadline) {
			markedFiles = append(markedFiles, k)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, k := range markedFiles {
		if err := tx.Bucket(dbFileBucket).Delete(k); err != nil {
			return 0, err
		}
	}

	return len(markedFiles), tx.Commit()
}

// GetOldFiles returns maxCount or less expired files
func (d BoltDB) GetOldFiles(maxCount int, deadline time.Time) (files []File, err error) {
	stop := errors.New("stop")
	err = d.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(dbFileBucket).ForEach(func(k []byte, v []byte) error {
			var f File
			f, err = UnmarshalFile(v)
			if err != nil {
				return err
			}
			if f.LastUsageBefore(deadline) {
				files = append(files, f)
			}
			if len(files) >= maxCount {
				return stop
			}
			return nil
		})
	})
	if err == stop {
		err = nil
	}

	return files, err
}

// GetOldFilesCount count of files that LastUsage is older than deadline
func (d BoltDB) GetOldFilesCount(deadline time.Time) (count int64, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		wg := new(sync.WaitGroup)
		work := make(chan []byte)
		worker := func(w chan []byte) {
			var f File
			defer wg.Done()
			for v := range w {
				err = UnmarshalFileTo(v, &f)
				if err != nil {
					panic(err)
				}
				if f.LastUsageBefore(deadline) {
					atomic.AddInt64(&count, 1)
				}
			}
		}
		workers := runtime.GOMAXPROCS(0)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go worker(work)
		}
		err = tx.Bucket(dbFileBucket).ForEach(func(k []byte, v []byte) error {
			work <- v
			return nil
		})
		close(work)
		wg.Wait()
		return err
	})
	return count, err
}

// GetFilesCount is count of files in database
func (d BoltDB) GetFilesCount() (count int) {
	d.db.View(func(tx *bolt.Tx) error {
		count = tx.Bucket(dbFileBucket).Stats().KeyN
		return nil
	})
	return
}

func (d BoltDB) get(id []byte) (f File, err error) {
	tx, err := d.db.Begin(false)
	if err != nil {
		return f, err
	}
	data := tx.Bucket(dbFileBucket).Get(id)
	if len(data) == 0 {
		return f, ErrFileNotFound
	}
	return UnmarshalFile(data)
}
