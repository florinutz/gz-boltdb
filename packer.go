package gz_boltdb

import (
	"compress/gzip"
	"io/ioutil"
	"os"
	"time"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// &bolt.Options{Timeout: 1 * time.Second}
func Open(path string, mode os.FileMode, options *bolt.Options) (db *bolt.DB, tmpFile *os.File, err error) {
	return getOrCreateDb(path, mode, options)
}

func getOrCreateDb(gzFilename string, mode os.FileMode, options *bolt.Options) (*bolt.DB, *os.File, error) {
	db, tmpFile, err := loadDbFromGz(gzFilename)
	if err == nil {
		return db, tmpFile, nil
	}

	tmpFile, err = ioutil.TempFile("", "bolt-*.db")
	if err != nil {
		return nil, nil, errors.New("cannot create temporary file")
	}

	db, err = bolt.Open(tmpFile.Name(), mode, options)
	if err != nil {
		return nil, nil, errors.New("cannot create temporary db")
	}

	return db, tmpFile, nil
}

func loadDbFromGz(gzPath string) (db *bolt.DB, tmpFile *os.File, err error) {
	f, err := os.OpenFile(gzPath, os.O_RDONLY, 0700)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not open file '%s'", gzPath)
	}
	defer f.Close()

	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not instantiate gz reader from '%s'", gzPath)
	}

	var uncompressed []byte
	_, err = zr.Read(uncompressed)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not read gz contents from '%s'", gzPath)
	}

	// create in /tmp
	tmpFile, err = ioutil.TempFile("", "bolt-*.db")
	if err != nil {
		return nil, nil, errors.Wrap(err, "cannot create temporary file")
	}

	_, err = tmpFile.Write(uncompressed)
	if err != nil {
		return nil, nil, errors.Wrap(err, "can't write contents to tmp file")
	}

	db, err = bolt.Open(tmpFile.Name(), 0640, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, nil, errors.Wrapf(err, "couldn't create/open bolt db at path '%s'", gzPath)
	}

	return db, tmpFile, nil
}

func WriteToGz(db *bolt.DB, gzFilename string) error {
	return db.View(func(tx *bolt.Tx) error {
		f, err := os.OpenFile(gzFilename, os.O_WRONLY|os.O_CREATE, 0700)
		if err != nil {
			return err
		}
		defer f.Close()

		zw := gzip.NewWriter(f)
		zw.Comment = "unit test db"
		defer zw.Close()

		_, err = tx.WriteTo(zw)
		return err
	})
}
