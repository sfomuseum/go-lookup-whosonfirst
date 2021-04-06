package catalog

import (
	"context"
	"github.com/aaronland/go-roster"
	"net/url"
)

type Catalog interface {
	Load(string) (interface{}, bool)
	LoadOrStore(string, interface{}) (interface{}, bool)
	Delete(string)
	Range(func(key, value interface{}) bool) error
	Count() int32
}

var catalogs roster.Roster

type CatalogInitializationFunc func(ctx context.Context, uri string) (Catalog, error)

func RegisterCatalog(ctx context.Context, scheme string, init_func CatalogInitializationFunc) error {

	err := ensureCatalogRoster()

	if err != nil {
		return err
	}

	return catalogs.Register(ctx, scheme, init_func)
}

func ensureCatalogRoster() error {

	if catalogs == nil {

		r, err := roster.NewDefaultRoster()

		if err != nil {
			return err
		}

		catalogs = r
	}

	return nil
}

func NewCatalog(ctx context.Context, uri string) (Catalog, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	scheme := u.Scheme

	i, err := catalogs.Driver(ctx, scheme)

	if err != nil {
		return nil, err
	}

	init_func := i.(CatalogInitializationFunc)
	return init_func(ctx, uri)
}

func Catalogs() []string {
	ctx := context.Background()
	return catalogs.Drivers(ctx)
}
