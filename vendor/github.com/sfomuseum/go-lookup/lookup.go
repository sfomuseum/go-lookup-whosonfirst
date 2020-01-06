package lookup

import (
	"context"
	"io"
	_ "log"
)

type Catalog interface {
	Load(string) (interface{}, bool)
	LoadOrStore(string, interface{}) (interface{}, bool)
	Delete(string)
	Range(func(key, value interface{}) bool) error
	Count() int32
}

// this will/should probably be updated to use aaronland/go-roster but today it is not hence
// the clunky constructor-ing (20191223/thisisaaronland)

type LookerUpper interface {
	Open(context.Context, string) error
	Append(context.Context, Catalog, ...AppendLookupFunc) error
}

type AppendLookupFunc func(context.Context, Catalog, io.ReadCloser) error

func SeedCatalog(ctx context.Context, c Catalog, looker_uppers []LookerUpper, append_funcs []AppendLookupFunc) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done_ch := make(chan bool)
	err_ch := make(chan error)

	remaining := len(looker_uppers)

	for _, l := range looker_uppers {

		go func(l LookerUpper) {

			err := l.Append(ctx, c, append_funcs...)

			if err != nil {
				err_ch <- err
			}

			done_ch <- true

		}(l)
	}

	for remaining > 0 {
		select {
		case <-done_ch:
			remaining -= 1
		case err := <-err_ch:
			return err
		default:
			// pass
		}
	}

	return nil
}
