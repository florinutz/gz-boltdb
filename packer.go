package gz_boltdb

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// &bolt.Options{Timeout: 1 * time.Second}
func Open(path string, mode os.FileMode, options *bolt.Options) (db *bolt.DB, tmpFile *os.File, err error) {
	db, tmpFile, err = loadDbFromGz(path)
	if err == nil {
		return
	}

	if info, err := os.Stat(path); !os.IsNotExist(err) {
		err = fmt.Errorf("file '%s' exists, but the db was not loaded\nFileInfo: %v", path, info)
		return
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

// loadDbFromGz unpacks and loads a bolt database
func loadDbFromGz(gzPath string) (db *bolt.DB, tmpFile *os.File, err error) {
	var f *os.File
	if f, err = os.OpenFile(gzPath, os.O_RDONLY, 0700); err != nil {
		err = errors.Wrapf(err, "could not open file '%s'", gzPath)
		return
	}
	defer f.Close()

	zr, err := gzip.NewReader(f)
	if err != nil {
		err = errors.Wrapf(err, "could not instantiate gz reader from '%s'", gzPath)
		return
	}

	var uncompressed []byte
	n, err := zr.Read(uncompressed)
	if err != nil {
		err = errors.Wrapf(err, "could not read gz contents from '%s'", gzPath)
		return
	}
	if n == 0 {
		err = errors.New("nothing was uncompressed")
		return
	}

	// create in /tmp
	tmpFile, err = ioutil.TempFile("", "gz-bolt-*.db")
	if err != nil {
		err = errors.Wrap(err, "cannot create temporary file")
		return
	}

	_, err = tmpFile.Write(uncompressed)
	if err != nil {
		err = errors.Wrap(err, "can't write contents to tmp file")
		return
	}

	db, err = bolt.Open(tmpFile.Name(), 0640, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		err = errors.Wrapf(err, "couldn't create/open bolt db at path '%s'", gzPath)
		return
	}

	return
}

func WriteToGz(db *bolt.DB, gzFilename string) error {
	return db.View(func(tx *bolt.Tx) error {
		f, err := os.OpenFile(gzFilename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0700)
		if err != nil {
			return err
		}
		defer f.Close()

		zw := gzip.NewWriter(f)
		zw.Comment = "unit test db"
		defer zw.Close()

		n, err := tx.WriteTo(zw)
		if n == 0 {
			return errors.New("nothing was compressed")
		}

		return err
	})
}
