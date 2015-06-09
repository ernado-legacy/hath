package hath

import (
	"encoding/hex"
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

// FileRecord stores info about file
type FileRecord struct {
	ID        string    `json:"-"`
	LastUsage time.Time `json:"last_usage"`
	Size      int64     `json:"size"`
}

// ByteID returns
func (f FileRecord) ByteID() []byte {
	d, err := hex.DecodeString(f.ID)
	if err != nil {
		panic("hath => bad id")
	}
	return d
}

// DB stores info about files in cache
type DB struct {
	db *bolt.DB
}

// NewDB new db
func NewDB(dbPath string) (d *DB, err error) {
	d = new(DB)
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

	return d, err
}

func (d DB) add(f FileRecord) error {
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

func (d DB) get(id []byte) (f FileRecord, err error) {
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
