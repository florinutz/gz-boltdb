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
	"go.etcd.io/bbolt"
)

// createDbInTempFile creates a temporary file, get its name, removes it,
// then creates the db in that exact location
func createDbInTempFile(options *bbolt.Options) (*bbolt.DB, error) {
	tmpFile, err := ioutil.TempFile("", "bolt-*.db")
	if err != nil {
		return nil, errors.New("cannot create temporary file")
	}
	tmpFileName := tmpFile.Name()
	tmpFile.Close()
	_ = os.Remove(tmpFile.Name())

	db, err := bbolt.Open(tmpFileName, 0600, options)
	if err != nil {
		return nil, errors.New("cannot create temporary db")
	}

	return db, nil
}

// openGz unpacks and loads a bolt database
func openGz(gzPath string) (db *bbolt.DB, err error) {
	tmpFile, _, err := unpackToTMP(gzPath)
	if err != nil {
		err = errors.Wrapf(err, "couldn't unpack '%s'", gzPath)
		return
	}
	defer tmpFile.Close()

	db, err = bbolt.Open(tmpFile.Name(), 0640, &bbolt.Options{Timeout: 1 * time.Second})
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
