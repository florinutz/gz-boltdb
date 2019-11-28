package gzbolt

import (
	"compress/gzip"
	"fmt"
	"os"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// Open behaves like bolt's Open, but works by unpacking the path gz file into a temporary file
// and using that as the db.
// The options param is used while opening the database in the temporary file.
// If the strict flag is set, the function will return an error instead of create a tmp database.
func Open(gzFilePath string, options *bolt.Options, strict bool) (db *bolt.DB, err error) {
	db, err = openGz(gzFilePath)
	if err != nil {
		if strict {
			return nil, err
		}
		return createDbInTempFile(options)
	}
	return
}

// Write dumps the db to an open file.
// Will return err if the file is not writeable.
// gzHeaders can be passed.
/** You can create or overwrite a target file like this:
f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
if err != nil {
	return err
}
defer f.Close()
*/
func Write(db *bolt.DB, file *os.File, gzHeader *gzip.Header) error {
	return db.View(func(tx *bolt.Tx) error {
		zw := gzip.NewWriter(file)
		if gzHeader != nil {
			zw.Header = *gzHeader
		}
		defer zw.Close()

		n, err := tx.WriteTo(zw)
		if err != nil {
			return fmt.Errorf("error while trying to write gz: %w", err)
		}
		if n == 0 {
			return errors.New("nothing was compressed")
		}

		return err
	})
}
