package hath

import (
	"bytes"
	"encoding/binary"
	"log"
	"time"

	"github.com/boltdb/bolt"
)

const (
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
	Add(f File) error
	AddBatch(f []File) error
	Use(f File) error
	Remove(f File) error
	RemoveBatch(f []File) error
	Close() error
	Count() int
}

// BoltDB stores info about files in cache
// stores data in b-tree structure
// stores index on LastUsage+Hash
// implements DataBase interface
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

// Close closes boltdb internal database
func (d BoltDB) Close() error {
	return d.db.Close()
}

// Add inserts file info to db
func (d BoltDB) Add(f File) error {
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
		if err := index.Put(f.indexKey(), nil); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// RemoveBatch remove file and corresponding index records
func (d BoltDB) RemoveBatch(files []File) error {
	if len(files) == 0 {
		return nil
	}
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fileBucket := tx.Bucket(dbFileBucket)
	indexBucket := tx.Bucket(dbTimeIndexBucket)

	for _, f := range files {
		if err := fileBucket.Delete(f.ByteID()); err != nil {
			return err
		}
		if err := indexBucket.Delete(f.indexKey()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Remove deletes file info and index from db
func (d BoltDB) Remove(f File) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fileBucket := tx.Bucket(dbFileBucket)
	indexBucket := tx.Bucket(dbTimeIndexBucket)

	if err := fileBucket.Delete(f.ByteID()); err != nil {
		return err
	}
	if err := indexBucket.Delete(f.indexKey()); err != nil {
		return err
	}
	return tx.Commit()
}

// Use updates LastUsage of provided file
func (d BoltDB) Use(f File) error {
	lastUsage := time.Now().Unix()
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fileBucket := tx.Bucket(dbFileBucket)
	indexBucket := tx.Bucket(dbTimeIndexBucket)

	// getting file from database
	// for consistency
	f, err = d.Get(f.ByteID())
	if err != nil {
		return err
	}

	if err := indexBucket.Delete(f.indexKey()); err != nil {
		return err
	}
	f.LastUsage = lastUsage
	if err := indexBucket.Put(f.indexKey(), nil); err != nil {
		return err
	}
	if err := fileBucket.Put(f.ByteID(), f.Bytes()); err != nil {
		return err
	}

	return tx.Commit()
}

// getIndexStart returns range start key
func getIndexStart(deadline time.Time) []byte {
	return make([]byte, timeBytes+HashSize)
}

// getIndexEnd returns range max possible key,
// which corresponding file is lastUsage <= deadline
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

// getIDFromIndexKey extracts id from index key byte array
// index key contains joined timestamp and file id
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

		// iterating over range of keys in index bucket
		// min - start of the range, slice of zeroes
		// max - end of the range, last key with possible LastUsage <= deadline
		// saving all file ids to hashes slice
		c := tx.Bucket(dbTimeIndexBucket).Cursor()
		for k, _ := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, _ = c.Next() {
			hashes = append(hashes, getIDFromIndexKey(k))
			if len(hashes) >= maxCount {
				break
			}
		}

		// nothing found
		if len(hashes) == 0 {
			return nil
		}

		// pre-allocating memory
		files = make([]File, len(hashes))

		// loading file infos with ids from hashes
		bucket := tx.Bucket(dbFileBucket)
		var f File
		for i, k := range hashes {
			data := bucket.Get(k)
			// possible consistency failure
			if data == nil {
				return ErrFileNotFound
			}
			// deserializing file info
			if err = UnmarshalFileTo(data, &f); err != nil {
				return err
			}
			files[i] = f
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

// Count is count of files in database
func (d BoltDB) Count() (count int) {
	d.db.View(func(tx *bolt.Tx) error {
		count = tx.Bucket(dbFileBucket).Stats().KeyN
		return nil
	})
	return
}

// Get loads file from database
func (d BoltDB) Get(id []byte) (f File, err error) {
	var data []byte
	err = d.db.View(func(tx *bolt.Tx) error {
		data = tx.Bucket(dbFileBucket).Get(id)

		if data == nil || len(data) == 0 {
			return ErrFileNotFound
		}

		return nil
	})

	if err != nil {
		return f, err
	}

	return UnmarshalFile(data)
}
