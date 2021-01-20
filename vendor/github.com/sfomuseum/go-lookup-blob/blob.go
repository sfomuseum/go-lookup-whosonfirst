package blob

import (
	"bytes"
	"context"
	"github.com/sfomuseum/go-lookup"
	gc_blob "gocloud.dev/blob"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
)

type BlobLookerUpper struct {
	lookup.LookerUpper
	bucket *gc_blob.Bucket
}

func init() {

	ctx := context.Background()
	err := lookup.RegisterLookerUpper(ctx, "blob", NewBlobLookerUpper)

	if err != nil {
		panic(err)
	}
}

func NewBlobLookerUpperWithBucket(ctx context.Context, bucket *gc_blob.Bucket) lookup.LookerUpper {

	l := &BlobLookerUpper{
		bucket: bucket,
	}

	return l
}

func NewBlobLookerUpper(ctx context.Context, uri string) (lookup.LookerUpper, error) {

	bucket, err := gc_blob.OpenBucket(ctx, uri)

	if err != nil {
		return nil, err
	}

	l := &BlobLookerUpper{
		bucket: bucket,
	}

	return l, nil
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
