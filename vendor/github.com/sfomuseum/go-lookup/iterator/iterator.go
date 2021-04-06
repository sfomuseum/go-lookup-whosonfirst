package iterator

import (
	"context"
	"github.com/aaronland/go-roster"
	"github.com/sfomuseum/go-lookup/catalog"
	"io"
	"net/url"
)

type AppendLookupFunc func(context.Context, catalog.Catalog, io.ReadCloser) error

type Iterator interface {
	Append(context.Context, catalog.Catalog, ...AppendLookupFunc) error
}

var iterators roster.Roster

type IteratorInitializationFunc func(ctx context.Context, uri string) (Iterator, error)

func RegisterIterator(ctx context.Context, scheme string, init_func IteratorInitializationFunc) error {

	err := ensureIteratorRoster()

	if err != nil {
		return err
	}

	return iterators.Register(ctx, scheme, init_func)
}

func ensureIteratorRoster() error {

	if iterators == nil {

		r, err := roster.NewDefaultRoster()

		if err != nil {
			return err
		}

		iterators = r
	}

	return nil
}

func NewIterator(ctx context.Context, uri string) (Iterator, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	scheme := u.Scheme

	i, err := iterators.Driver(ctx, scheme)

	if err != nil {
		return nil, err
	}

	init_func := i.(IteratorInitializationFunc)
	return init_func(ctx, uri)
}

func Iterators() []string {
	ctx := context.Background()
	return iterators.Drivers(ctx)
}
