package hath

import (
	"encoding/json"
	"time"

	"github.com/boltdb/bolt"
)

const (
	dbDir      = "db"
	dbFileMode = 0600
)

var (
	dbFileBucket = []byte("files")
	dbOptions    = bolt.Options{Timeout: 1 * time.Second}
)

type DataBase interface {
}

// DB stores info about files in cache
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
	data, err := json.Marshal(f)
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

// Add inserts slice of files into db
func (d BoltDB) AddBatch(files []File) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	bucket := tx.Bucket(dbFileBucket)
	for _, f := range files {
		data, err := json.Marshal(f)
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
		if err := json.Unmarshal(v, &f); err != nil {
			return err
		}
		if f.LastUsage.Before(deadline) {
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

func (d BoltDB) get(id []byte) (f File, err error) {
	tx, err := d.db.Begin(false)
	if err != nil {
		return f, err
	}
	data := tx.Bucket(dbFileBucket).Get(id)
	if len(data) == 0 {
		return f, ErrFileNotFound
	}
	return f, json.Unmarshal(data, &f)
}