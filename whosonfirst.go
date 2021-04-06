package whosonfirst

import (
	_ "gocloud.dev/blob/fileblob"
)

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/sfomuseum/go-lookup"
	_ "github.com/sfomuseum/go-lookup-blob"
	_ "github.com/sfomuseum/go-lookup-git"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/sfomuseum/go-lookup/iterator"
	"github.com/tidwall/pretty"
	"net/url"
)

type CatalogOptions struct {
	Catalog     catalog.Catalog
	AppendFuncs []iterator.AppendLookupFunc
	Iterators   []iterator.Iterator
}

func DefaultCatalogOptions() (*CatalogOptions, error) {

	ctx := context.Background()

	c, err := catalog.NewCatalog(ctx, "syncmap://")

	if err != nil {
		return nil, err
	}

	funcs := make([]iterator.AppendLookupFunc, 0)
	lookers := make([]iterator.Iterator, 0)

	opts := &CatalogOptions{
		Catalog:     c,
		AppendFuncs: funcs,
		Iterators:   lookers,
	}

	return opts, nil
}

func NewLookupURI(scheme string, lu_scheme string, uri string) string {

	u := url.URL{}
	u.Scheme = scheme
	u.Host = lu_scheme

	p := url.Values{}
	p.Set("uri", uri)

	u.RawQuery = p.Encode()
	return u.String()

}

func NewCatalog(ctx context.Context, uri string) (catalog.Catalog, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	var opts *CatalogOptions
	var opts_err error

	switch u.Scheme {
	case "fingerprint":
		opts, opts_err = DefaultFingerprintCatalogOptions()
	case "imagehash":
		opts, opts_err = DefaultImageHashCatalogOptions()
	case "media":
		opts, opts_err = DefaultFingerprintCatalogOptions()

		if opts_err == nil {
			opts.AppendFuncs = append(opts.AppendFuncs, AppendImageHashFunc)
		}

	default:
		return nil, errors.New("Unsupported lookup")
	}

	if opts_err != nil {
		return nil, opts_err
	}

	q := u.Query()
	lu_uri := q.Get("uri")

	lu, err := iterator.NewIterator(ctx, lu_uri)

	if err != nil {
		return nil, err
	}

	opts.Iterators = append(opts.Iterators, lu)
	return NewCatalogWithOptions(ctx, opts)
}

func NewCatalogWithOptions(ctx context.Context, opts *CatalogOptions) (catalog.Catalog, error) {

	err := lookup.SeedCatalog(ctx, opts.Catalog, opts.Iterators, opts.AppendFuncs)

	if err != nil {
		return nil, err
	}

	return opts.Catalog, nil
}

func MarshalCatalog(c catalog.Catalog) ([]byte, error) {

	lookup := make(map[string]interface{})

	c.Range(func(key interface{}, value interface{}) bool {
		gate_name := key.(string)
		lookup[gate_name] = value
		return true
	})

	body, err := json.Marshal(lookup)

	if err != nil {
		return nil, err
	}

	body = pretty.Pretty(body)
	return body, nil
}
