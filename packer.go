package gzbolt

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

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

// createDbInTempFile creates a temporary file, get its name, removes it,
// then creates the db in that exact location
func createDbInTempFile(options *bolt.Options) (*bolt.DB, error) {
	tmpFile, err := ioutil.TempFile("", "bolt-*.db")
	if err != nil {
		return nil, errors.New("cannot create temporary file")
	}
	tmpFileName := tmpFile.Name()
	tmpFile.Close()
	_ = os.Remove(tmpFile.Name())

	db, err := bolt.Open(tmpFileName, 0600, options)
	if err != nil {
		return nil, errors.New("cannot create temporary db")
	}

	return db, nil
}

// openGz unpacks and loads a bolt database
func openGz(gzPath string) (db *bolt.DB, err error) {
	tmpFile, _, err := unpackToTMP(gzPath)
	if err != nil {
		err = errors.Wrapf(err, "couldn't unpack '%s'", gzPath)
		return
	}
	defer tmpFile.Close()

	db, err = bolt.Open(tmpFile.Name(), 0640, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		err = errors.Wrapf(err, "couldn't create/open bolt db at path '%s'", gzPath)
		return
	}

	return
}

func unpackToTMP(gzPath string) (tmpFile *os.File, written int64, err error) {
	var f *os.File
	if f, err = os.Open(gzPath); err != nil {
		err = fmt.Errorf("could not open file '%s' for reading: %w", gzPath, err)
		return
	}
	defer f.Close()

	// unpack in /tmp
	tmpFile, err = ioutil.TempFile("", "gz-bolt-*.db")
	if err != nil {
		err = fmt.Errorf("cannot create temporary file: %w", err)
		return
	}

	w := bufio.NewWriter(tmpFile)

	written, err = unpackStreams(f, w)

	return
}

// WriteToGz dumps the db to a gz file at path. The file is overwritten if it exists. gzHeaders can be passed.
func WriteToGz(db *bolt.DB, path string, perm os.FileMode, gzHeader *gzip.Header) error {
	return db.View(func(tx *bolt.Tx) error {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
		if err != nil {
			return err
		}
		defer f.Close()

		zw := gzip.NewWriter(f)
		if gzHeader != nil {
			zw.Header = *gzHeader
		}
		defer zw.Close()

		n, err := tx.WriteTo(zw)
		if n == 0 {
			return errors.New("nothing was compressed")
		}

		return err
	})
}

// unpackStreams unpacks gz from reader to writer and returns the number of bytes written.
func unpackStreams(r io.Reader, w io.Writer) (written int64, err error) {
	var zr *gzip.Reader
	zr, err = gzip.NewReader(r)
	if err != nil {
		err = fmt.Errorf("could not instantiate gz reader: %w", err)
		return
	}
	defer zr.Close()

	written, err = io.Copy(w, zr)
	if err != nil {
		err = fmt.Errorf("could not read gz contents: %w", err)
		return
	}
	if written == 0 {
		err = fmt.Errorf("nothing was uncompressed")
		return
	}

	return
}
