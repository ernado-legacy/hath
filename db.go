package hath

import (
	"bytes"
	"encoding/binary"
	"log"
	"time"

	"github.com/boltdb/bolt"
)

const (
	dbDir      = "db"
	dbFileMode = 0600
	dbBulkSize = 10000
	timeBytes  = 8
)

var (
	dbFileBucket      = []byte("files")
	dbTimeIndexBucket = []byte("last_usage")
	dbOptions         = bolt.Options{Timeout: 1 * time.Second}
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
	_, err = tx.CreateBucketIfNotExists(dbTimeIndexBucket)
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
	if err := tx.Bucket(dbTimeIndexBucket).Put(f.indexKey(), f.ByteID()); err != nil {
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
	index := tx.Bucket(dbTimeIndexBucket)
	for _, f := range files {
		data, err := f.Marshal()
		if err != nil {
			return err
		}
		if err := bucket.Put(f.ByteID(), data); err != nil {
			return err
		}
		log.Printf("index: %x => %x", f.indexKey()[:8], f.ByteID())
		if err := index.Put(f.indexKey(), f.ByteID()); err != nil {
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

func getIndexStart(deadline time.Time) []byte {
	timeBytes := make([]byte, timeBytes)
	// binary.BigEndian.PutUint64(timeBytes, uint64(deadline.Unix()))
	hashBytes := make([]byte, HashSize)
	elems := [][]byte{
		timeBytes,
		hashBytes,
	}
	return bytes.Join(elems, nil)
}

func getIndexEnd(deadline time.Time) []byte {
	timeBytes := make([]byte, timeBytes)
	binary.BigEndian.PutUint64(timeBytes, uint64(deadline.Unix()))
	hashBytes := make([]byte, HashSize)
	for i := range hashBytes {
		hashBytes[i] = 255
	}
	elems := [][]byte{
		timeBytes,
		hashBytes,
	}
	return bytes.Join(elems, nil)
}

func getIDFromIndexKey(id []byte) []byte {
	hash := make([]byte, HashSize)
	copy(hash, id[timeBytes:])
	return hash
}

// GetOldFiles returns maxCount or less expired files
func (d BoltDB) GetOldFiles(maxCount int, deadline time.Time) (files []File, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		var hashes [][]byte
		min := getIndexStart(deadline)
		max := getIndexEnd(deadline)
		c := tx.Bucket(dbTimeIndexBucket).Cursor()
		for k, _ := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, _ = c.Next() {
			hashes = append(hashes, getIDFromIndexKey(k))
			if len(hashes) > maxCount {
				break
			}
		}
		bucket := tx.Bucket(dbFileBucket)
		var f File
		for _, k := range hashes {
			data := bucket.Get(k)
			if data == nil {
				return ErrFileNotFound
			}
			if err = UnmarshalFileTo(data, &f); err != nil {
				return err
			}
			files = append(files, f)
		}
		return nil
	})
	return files, err
}

// GetOldFilesCount count of files that LastUsage is older than deadline
func (d BoltDB) GetOldFilesCount(deadline time.Time) (count int64, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		min := getIndexStart(deadline)
		max := getIndexEnd(deadline)
		c := tx.Bucket(dbTimeIndexBucket).Cursor()
		for k, _ := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, _ = c.Next() {
			count++
		}
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
