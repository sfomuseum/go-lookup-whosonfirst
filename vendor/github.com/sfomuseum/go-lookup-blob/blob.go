package blob

import (
	"bytes"
	"context"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/sfomuseum/go-lookup/iterator"
	gc_blob "gocloud.dev/blob"
	"io"
	"io/ioutil"
	"log"
	"path/filepath"
)

type BlobIterator struct {
	iterator.Iterator
	bucket *gc_blob.Bucket
}

func init() {

	ctx := context.Background()
	err := iterator.RegisterIterator(ctx, "blob", NewBlobIterator)

	if err != nil {
		panic(err)
	}
}

func NewBlobIteratorWithBucket(ctx context.Context, bucket *gc_blob.Bucket) iterator.Iterator {

	l := &BlobIterator{
		bucket: bucket,
	}

	return l
}

func NewBlobIterator(ctx context.Context, uri string) (iterator.Iterator, error) {

	bucket, err := gc_blob.OpenBucket(ctx, uri)

	if err != nil {
		return nil, err
	}

	l := &BlobIterator{
		bucket: bucket,
	}

	return l, nil
}

func (l *BlobIterator) Append(ctx context.Context, lu catalog.Catalog, append_funcs ...iterator.AppendLookupFunc) error {

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
