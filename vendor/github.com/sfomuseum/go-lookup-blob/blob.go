package blob

import (
	"bytes"
	"context"
	gc_blob "gocloud.dev/blob"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
	"github.com/sfomuseum/go-lookup"
)

type BlobLookerUpper struct {
	lookup.LookerUpper
	bucket *gc_blob.Bucket
}

func NewBlobLookerUpper(ctx context.Context) lookup.LookerUpper {
	l := &BlobLookerUpper{}
	return l
}

func NewBlobLookerUpperWithBucket(ctx context.Context, bucket *gc_blob.Bucket) lookup.LookerUpper {

	l := &BlobLookerUpper{
		bucket: bucket,
	}

	return l
}

func (l *BlobLookerUpper) Open(ctx context.Context, uri string) error {

	if l.bucket == nil {

		bucket, err := gc_blob.OpenBucket(ctx, uri)

		if err != nil {
			return err
		}

		l.bucket = bucket
	}

	return nil
}

func (l *BlobLookerUpper) Append(ctx context.Context, lu lookup.Catalog, append_funcs ...lookup.AppendLookupFunc) error {

	bucket_iter := l.bucket.List(nil)

	for {
		obj, err := bucket_iter.Next(ctx)

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		if filepath.Ext(obj.Key) != ".geojson" {
			continue
		}

		fh, err := l.bucket.NewReader(ctx, obj.Key, nil)

		if err != nil {
			return err
		}

		defer fh.Close()

		body, err := ioutil.ReadAll(fh)

		if err != nil {
			return err
		}

		for _, f := range append_funcs {

			br := bytes.NewReader(body)
			fh := ioutil.NopCloser(br)

			err := f(ctx, lu, fh)

			if err != nil {
				log.Printf("BLOB %s: %s\n", obj.Key, err)
			}
		}

	}

	return nil
}
