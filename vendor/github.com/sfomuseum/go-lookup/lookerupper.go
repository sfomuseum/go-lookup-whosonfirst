package lookup

import (
	"context"
	"github.com/aaronland/go-roster"
	"io"
	"net/url"
)

type LookerUpper interface {
	Append(context.Context, Catalog, ...AppendLookupFunc) error
}

type AppendLookupFunc func(context.Context, Catalog, io.ReadCloser) error

var lookeruppers roster.Roster

type LookerUpperInitializationFunc func(ctx context.Context, uri string) (LookerUpper, error)

func RegisterLookerUpper(ctx context.Context, scheme string, init_func LookerUpperInitializationFunc) error {

	err := ensureLookerUpperRoster()

	if err != nil {
		return err
	}

	return lookeruppers.Register(ctx, scheme, init_func)
}

func ensureLookerUpperRoster() error {

	if lookeruppers == nil {

		r, err := roster.NewDefaultRoster()

		if err != nil {
			return err
		}

		lookeruppers = r
	}

	return nil
}

func NewLookerUpper(ctx context.Context, uri string) (LookerUpper, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	scheme := u.Scheme

	i, err := lookeruppers.Driver(ctx, scheme)

	if err != nil {
		return nil, err
	}

	init_func := i.(LookerUpperInitializationFunc)
	return init_func(ctx, uri)
}

func LookerUppers() []string {
	ctx := context.Background()
	return lookeruppers.Drivers(ctx)
}
