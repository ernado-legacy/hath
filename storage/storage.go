// storage package defines a way to save, load, list and delete files for distributed file sharing service
// basically it is a abstraction layer
package storage

import "time"

// File is stored in service
type File struct {
	Name string
	Size int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

