package http

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/florinutz/gz-boltdb"
	"github.com/pkg/errors"
	"go.etcd.io/bbolt"
)

// as I can't gob encode the response body (which is a Reader) or the TLS field, I will use this instead
type GobResponse struct {
	Status           string
	StatusCode       int
	Proto            string
	ProtoMajor       int
	ProtoMinor       int
	Header           http.Header
	Body             []byte
	ContentLength    int64
	TransferEncoding []string
	Close            bool
	Uncompressed     bool
	Trailer          http.Header
	Request          *http.Request
}

func (rg *GobResponse) FromResponse(r http.Response) error {
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	rg.Status = r.Status
	rg.StatusCode = r.StatusCode
	rg.Proto = r.Proto
	rg.ProtoMajor = r.ProtoMajor
	rg.ProtoMinor = r.ProtoMinor
	rg.Header = r.Header
	rg.Body = bodyBytes
	rg.ContentLength = r.ContentLength
	rg.TransferEncoding = r.TransferEncoding
	rg.Close = r.Close
	rg.Uncompressed = r.Uncompressed
	rg.Trailer = r.Trailer
	rg.Request = r.Request

	return nil
}

func (rg *GobResponse) ToResponse() (r *http.Response) {
	r = new(http.Response)
	r.Status = rg.Status
	r.StatusCode = rg.StatusCode
	r.Proto = rg.Proto
	r.ProtoMajor = rg.ProtoMajor
	r.ProtoMinor = rg.ProtoMinor
	r.Header = rg.Header
	r.Body = ioutil.NopCloser(bytes.NewReader(rg.Body))
	r.ContentLength = rg.ContentLength
	r.TransferEncoding = rg.TransferEncoding
	r.Close = rg.Close
	r.Uncompressed = rg.Uncompressed
	r.Trailer = rg.Trailer
	r.Request = rg.Request

	return nil
}

func FetchUrls(requests []*http.Request, client http.Client) (responses []*http.Response, errs []error) {
	type reqRespErr struct {
		req  *http.Request
		resp *http.Response
		err  error
	}

	c := make(chan reqRespErr, len(requests))

	for i, req := range requests {
		go func(req *http.Request, output chan<- reqRespErr, id int) {
			resp, err := client.Do(req)
			output <- reqRespErr{
				req:  req,
				resp: resp,
				err:  err,
			}
		}(req, c, i)
	}

	timeout := time.Duration(3*len(requests)) * time.Second

	for i := 0; i < len(requests); i++ {
		select {
		case rre := <-c:
			if rre.err != nil {
				errs = append(errs, errors.Wrapf(rre.err, "req to '%s' failed", rre.req.URL.String()))
				continue
			}
			responses = append(responses, rre.resp)
		case <-time.After(timeout):
			errs = append(errs, fmt.Errorf("timeout after %s", timeout))
		}
	}

	return
}

func DumpResponses(reqs []*http.Request, outputPath string, bucketName string, gzHeader *gzip.Header) (err error) {
	// load db from compressed outputPath of create a new tmp file for it
	db, err := gzbolt.Open(outputPath, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return
	}
	defer db.Close()

	responses, errs := FetchUrls(reqs, *http.DefaultClient)
	for _, err := range errs {
		fmt.Fprintln(os.Stderr, err)
	}
	defer func(responses []*http.Response) {
		for _, r := range responses {
			r.Body.Close()
		}
	}(responses)

	if err = db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}

		gob.Register(http.Request{})

		for _, resp := range responses {
			key, err := getKeyBytes(*resp.Request)
			if err != nil {
				return errors.Wrap(err, "couldn't generate key for request")
			}

			var content []byte
			content, err = encodeResponse(resp)
			if err != nil {
				return errors.Wrap(err, "couldn't encode response to bytes")
			}

			if err = bucket.Put(key, content); err != nil {
				return errors.Wrapf(err, "couldn't save response into bucket '%s'", bucketName)
			}
		}

		return nil
	}); err != nil {
		return
	}

	err = gzbolt.WriteToGz(db, outputPath, 0700, gzHeader)
	if err != nil {
		return
	}

	return
}

func getKeyBytes(req http.Request) (key []byte, err error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err = encoder.Encode(req)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't encode a key")
	}
	b := sha256.Sum256(buf.Bytes())

	return b[:], nil
}

func encodeResponse(resp *http.Response) (content []byte, err error) {
	responseForBin := GobResponse{}

	gob.Register(responseForBin)

	err = responseForBin.FromResponse(*resp)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't prepare response for converting to binary")
	}

	var encodedBuffer bytes.Buffer
	encoder := gob.NewEncoder(&encodedBuffer)

	err = encoder.Encode(responseForBin)
	if err != nil {
		return
	}

	content = encodedBuffer.Bytes()

	return
}

func decodeResponse(from []byte) (*http.Response, error) {
	responseForBin := GobResponse{}

	// gob.Register(responseForBin)

	decodedBuffer := bytes.NewReader(from)
	decoder := gob.NewDecoder(decodedBuffer)

	err := decoder.Decode(responseForBin)
	if err != nil {
		return nil, err
	}

	return responseForBin.ToResponse(), nil
}

func GetResponsesFromDB(db *bbolt.DB, bucketName []byte) (responses []*http.Response, err error) {
	err = db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(bucketName))
		if bucket == nil {
			return fmt.Errorf("no such bucket '%s'", bucketName)
		}
		if err := bucket.ForEach(func(k, v []byte) error {
			resp, err := decodeResponse(v)
			if err != nil {
				return err
			}
			responses = append(responses, resp)

			return nil
		}); err != nil {
			return err
		}

		return nil
	})

	return
}
