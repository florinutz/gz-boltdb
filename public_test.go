package gzbolt_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	. "github.com/florinutz/gz-boltdb"
	bolt "go.etcd.io/bbolt"
)

const (
	Bucket = "galeata"
	Data   = "smth"
)

func TestPackUnpack(t *testing.T) {
	db, err := newDbWithData(&bolt.Options{Timeout: 1 * time.Second}, Data)
	if err != nil {
		t.Fatalf(fmt.Errorf("can't get a new bolt db: %w", err).Error())
	}
	defer db.Close()

	type args struct {
		db       *bolt.DB
		gzHeader *gzip.Header
	}
	type test struct {
		name string
		args args
	}
	var tests = []test{
		{
			name: "basic",
			args: args{
				db: db,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpfile, err := ioutil.TempFile("", "gz-*")
			if err != nil {
				t.Fatalf("can't create tmp gz file")
			}
			defer os.Remove(tmpfile.Name())

			// Write
			if err := Write(tt.args.db, tmpfile, tt.args.gzHeader); err != nil {
				t.Fatalf(fmt.Errorf("error during write: %w", err).Error())
			}
			t.Logf("size of tmp file after write: %d", mustGetFileStat(t, tmpfile).Size())

			// Open
			db, err = Open(tmpfile.Name(), nil, false)
			if err != nil {
				t.Fatalf(fmt.Errorf("error at open: %w", err).Error())
			}

			t.Logf("successfully wrote and then opened %s", db)

			if err = dbHasData(db, Data); err != nil {
				t.Fatalf(fmt.Errorf("data check failed: %w", err).Error())
			}

		})
	}
}

// checks if db's Bucket data
func dbHasData(db *bolt.DB, data string) error {
	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(Bucket))
		v := b.Get([]byte("data"))
		if data != string(v) {
			return fmt.Errorf("retrieved '%s', wanted '%s'", string(v), data)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// mustGetFileStat fails the test if it can't retrieve the size
func mustGetFileStat(t *testing.T, file *os.File) os.FileInfo {
	fileInfo, err := file.Stat()
	if err != nil {
		err = fmt.Errorf("couldn't obtain file stat: %w", err)
		t.Error(err)
	}
	return fileInfo
}

// initialize an empty bolt db in a temp file and add a piece of data
func newDbWithData(options *bolt.Options, data string) (*bolt.DB, error) {
	tmpFile, err := getTmpPath()
	if err != nil {
		return nil, fmt.Errorf("can't get a tmp file: %w", err)
	}

	db, err := bolt.Open(tmpFile, 0600, options)
	if err != nil {
		return nil, fmt.Errorf("can't create new db in tmp file: %w", err)
	}

	// add some data
	if err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte(Bucket))
		if err != nil {
			return fmt.Errorf("error creating bucket: %w", err)
		}
		err = b.Put([]byte("data"), []byte(data))
		if err != nil {
			return fmt.Errorf("error adding data to bucket: %w", err)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("error while adding data: %w", err)
	}

	return db, err
}

func getTmpPath() (path string, err error) {
	file, err := ioutil.TempFile(os.TempDir(), "bbolt-*")
	if err != nil {
		return "", fmt.Errorf("can't create tmp file: %w", err)
	}
	defer file.Close()

	return file.Name(), nil
}
