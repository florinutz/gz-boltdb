package gzbolt_test

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	. "github.com/florinutz/gz-boltdb"
	bolt "go.etcd.io/bbolt"
)

// Example shows
func ExampleWrite() {
	// create bolt db in a temp file and write a piece of data ("something") in a bucket
	db, err := newDbWithData(&bolt.Options{Timeout: 1 * time.Second}, "something")
	if err != nil {
		log.Fatalf(fmt.Errorf("problem with creating test db: %w", err).Error())
	}
	defer db.Close()

	// create a writeable *os.File in order to feed it to Write
	tmpfile, err := ioutil.TempFile("", "gz-*")
	if err != nil {
		log.Fatalf(fmt.Errorf("problem with obtaining a valid writeable *os.File: %w", err).Error())
	}
	defer os.Remove(tmpfile.Name())

	// Write gz to the previously created file.
	// The gz headers can be nil.
	if err := Write(db, tmpfile, &gzip.Header{Comment: "my precious bbolt dump"}); err != nil {
		log.Fatalf(fmt.Errorf("problem with writing db to: %w", err).Error())
	}

	// Open the freshly written archive
	db, err = Open(tmpfile.Name(), nil, false)
	if err != nil {
		log.Fatalf(fmt.Errorf("problem with opening the gz: %w", err).Error())
	}

	// check that it has "something" in it
	if err = dbHasData(db, "something"); err != nil {
		log.Fatalf(fmt.Errorf("problem with opening the gz: %w", err).Error())
	}

	fmt.Println("since we got to this point, both Write and Open funcs worked.")
	// Output: since we got to this point, both Write and Open funcs worked.
}
