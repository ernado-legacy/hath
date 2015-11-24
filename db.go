package hath

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"time"

	"github.com/boltdb/bolt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
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
	UseBatch(files []File) error
	Remove(f File) error
	RemoveBatch(f []File) error
	Close() error
	Count() int
	GetOldFiles(maxCount int, deadline time.Time) (files []File, err error)
	GetOldFilesCount(deadline time.Time) (count int64, err error)
	Size() (int64, error)
	Exists(f File) bool
	Get(id []byte) (File, error)
	GetBatch(files chan File, max int64) (err error)
}

// BoltDB stores info about files in cache
// stores data in b-tree structure
// stores index on LastUsage+Hash
// implements DataBase interface
type BoltDB struct {
	db *bolt.DB
}

// LevelDB implementation for database
type LevelDB struct {
	files *leveldb.DB
	index *leveldb.DB
}

// NewLevelDB creates dbPath.files and dbPath.index dbs
func NewLevelDB(dbPath string) (d *LevelDB, err error) {
	d = new(LevelDB)
	fileDB := dbPath + ".files"
	indexDB := dbPath + ".index"

	files, err := leveldb.OpenFile(fileDB, nil)
	if err != nil {
		return nil, err
	}
	d.files = files

	index, err := leveldb.OpenFile(indexDB, nil)
	if err != nil {
		return nil, err
	}
	d.index = index

	return d, nil
}

func (db LevelDB) GetOldFilesCount(deadline time.Time) (count int64, err error) {
	iter := db.index.NewIterator(&util.Range{
		Start: getIndexStart(deadline),
		Limit: getIndexEnd(deadline),
	}, nil)
	for iter.Next() {
		count++
	}
	iter.Release()
	return count, iter.Error()
}

func (db LevelDB) GetOldFiles(maxCount int, deadline time.Time) (files []File, err error) {
	var (
		f     File
		count int
	)
	iter := db.index.NewIterator(&util.Range{
		Start: getIndexStart(deadline),
		Limit: getIndexEnd(deadline),
	}, nil)
	for iter.Next() {
		f, err = db.Get(getIDFromIndexKey(iter.Value()))
		if err != nil {
			break
		}
		files = append(files, f)
		count++
		if count >= maxCount {
			break
		}
	}
	return files, err
}

func (db LevelDB) Count() (count int) {
	iter := db.files.NewIterator(nil, nil)
	for iter.Next() {
		count++
	}
	iter.Release()
	return count
}

func (db LevelDB) Size() (sum int64, err error) {
	iter := db.files.NewIterator(nil, nil)
	var f File
	for iter.Next() {
		if err := db.deserialize(iter.Key(), iter.Value(), &f); err != nil {
			return 0, err
		}
		sum += f.Size
	}
	iter.Release()
	return sum, nil
}

func (db LevelDB) Add(f File) error {
	if err := db.files.Put(f.ByteID(), db.serialize(f), nil); err != nil {
		return err
	}
	if err := db.index.Put(f.indexKey(), nil, nil); err != nil {
		return err
	}
	return nil
}

func (db LevelDB) AddBatch(f []File) error {
	batchFiles := new(leveldb.Batch)
	batchIndex := new(leveldb.Batch)
	for _, v := range f {
		batchFiles.Put(v.ByteID(), db.serialize(v))
		batchIndex.Put(v.indexKey(), nil)
	}
	if err := db.files.Write(batchFiles, nil); err != nil {
		return err
	}
	if err := db.index.Write(batchIndex, nil); err != nil {
		return err
	}
	return nil
}

func (db LevelDB) Remove(f File) error {
	if err := db.files.Delete(f.ByteID(), nil); err != nil {
		return err
	}
	return db.index.Delete(f.indexKey(), nil)
}

func (db LevelDB) RemoveBatch(files []File) error {
	for _, f := range files {
		if err := db.files.Delete(f.ByteID(), nil); err != nil {
			return err
		}
		if err := db.index.Delete(f.indexKey(), nil); err != nil {
			return err
		}
	}
	return nil
}

func (db LevelDB) Close() error {
	if err := db.index.Close(); err != nil {
		return err
	}
	return db.files.Close()
}

func (db LevelDB) Get(id []byte) (f File, err error) {
	var data []byte
	data, err = db.files.Get(id, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return f, ErrFileNotFound
		}
		return f, err
	}
	return f, db.deserialize(id, data, &f)
}

func (db LevelDB) GetBatch(files chan File, max int64) (err error) {
	defer close(files)
	iter := db.files.NewIterator(nil, nil)
	var f File
	for iter.Next() {
		if err := db.deserialize(iter.Key(), iter.Value(), &f); err != nil {
			return err
		}
		files <- f
	}
	iter.Release()
	return nil
}

func (db LevelDB) Exists(f File) bool {
	ret, _ := db.files.Has(f.ByteID(), nil)
	return ret
}

func (db LevelDB) Use(f File) error {
	lastUsage := time.Now().Unix()
	if err := db.index.Delete(f.indexKey(), nil); err != nil {
		return err
	}
	f.LastUsage = lastUsage
	if err := db.index.Put(f.indexKey(), nil, nil); err != nil {
		return err
	}
	return db.files.Put(f.ByteID(), db.serialize(f), nil)
}

func (db LevelDB) UseBatch(files []File) error {
	lastUsage := time.Now().Unix()
	for _, f := range files {
		if err := db.index.Delete(f.indexKey(), nil); err != nil {
			return err
		}
		f.LastUsage = lastUsage
		if err := db.index.Put(f.indexKey(), nil, nil); err != nil {
			return err
		}
		if err := db.files.Put(f.ByteID(), db.serialize(f), nil); err != nil {
			return err
		}
	}
	return nil
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

func (d BoltDB) serialize(f File) []byte {
	data := f.Bytes()
	result := make([]byte, len(data)-HashSize)
	copy(result[:], data[HashSize:])
	return result
}

func (_ LevelDB) serialize(f File) []byte {
	data := f.Bytes()
	result := make([]byte, len(data)-HashSize)
	copy(result[:], data[HashSize:])
	return result
}

func (d BoltDB) deserialize(k, v []byte, f *File) error {
	data := bytes.Join([][]byte{k, v}, nil)
	return FileFromBytesTo(data, f)
}

func (_ LevelDB) deserialize(k, v []byte, f *File) error {
	data := bytes.Join([][]byte{k, v}, nil)
	return FileFromBytesTo(data, f)
}

// Add inserts file info to db
func (d BoltDB) Add(f File) error {
	data := d.serialize(f)
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
		if err := bucket.Put(f.ByteID(), d.serialize(f)); err != nil {
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
	if err := fileBucket.Put(f.ByteID(), d.serialize(f)); err != nil {
		return err
	}

	return tx.Commit()
}

// UseBatch updates lastUsage for list of files
func (d BoltDB) UseBatch(files []File) error {
	lastUsage := time.Now().Unix()
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	fileBucket := tx.Bucket(dbFileBucket)
	indexBucket := tx.Bucket(dbTimeIndexBucket)

	for _, file := range files {
		// getting file from database
		// for consistency
		f, err := d.Get(file.ByteID())
		if err != nil {
			log.Println("db:", "err:", file, err)
			continue
		}

		if err := indexBucket.Delete(f.indexKey()); err != nil {
			log.Println("db:", "lastUsed index update failed:", f)
		}
		f.LastUsage = lastUsage
		if err := indexBucket.Put(f.indexKey(), nil); err != nil {
			return err
		}
		if err := fileBucket.Put(f.ByteID(), d.serialize(f)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// getIndexStart returns range start key
func getIndexStart(deadline time.Time) []byte {
	return make([]byte, timeBytes+HashSize)
}

// getIndexEnd returns range max possible key,
// which corresponding file has lastUsage <= deadline
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
			if err = d.deserialize(k, data, &f); err != nil {
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

// Size of all files in cache
func (d BoltDB) Size() (sum int64, err error) {
	err = d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(dbFileBucket)
		var f File
		return bucket.ForEach(func(k []byte, v []byte) error {
			if err = d.deserialize(k, v, &f); err != nil {
				return err
			}
			sum += f.Size
			return nil
		})
	})
	return sum, err
}

// GetBatch reads files and sends it to channel
func (d BoltDB) GetBatch(files chan File, max int64) (err error) {
	var count int64
	err = d.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(dbFileBucket)
		var f File
		return bucket.ForEach(func(k []byte, v []byte) error {
			count++
			if err = d.deserialize(k, v, &f); err != nil {
				return err
			}
			files <- f
			if count >= max && max != 0 {
				return io.EOF
			}
			return nil
		})
	})
	if err == io.EOF {
		err = nil
	}
	return err
}

// Count is count of files in database
func (d BoltDB) Count() (count int) {
	d.db.View(func(tx *bolt.Tx) error {
		count = tx.Bucket(dbFileBucket).Stats().KeyN
		return nil
	})
	return
}

// Exists return true if file exists in db
func (d BoltDB) Exists(f File) (exists bool) {
	d.db.View(func(tx *bolt.Tx) error {
		exists = len(tx.Bucket(dbFileBucket).Get(f.ByteID())) != 0
		return nil
	})
	return exists
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

	return f, d.deserialize(id, data, &f)
}
