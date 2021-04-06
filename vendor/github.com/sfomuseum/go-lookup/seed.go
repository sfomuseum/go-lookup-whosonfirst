package lookup

import (
	"context"
	"github.com/sfomuseum/go-lookup/catalog"
	"github.com/sfomuseum/go-lookup/iterator"
)

// Seed a Catalog instance with one or more Iterator instances. Catalogs are seeded according to rules defined in the list of AppendLookupFunc functions that are passed in to the method.
func SeedCatalog(ctx context.Context, c catalog.Catalog, iters []iterator.Iterator, append_funcs []iterator.AppendLookupFunc) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	done_ch := make(chan bool)
	err_ch := make(chan error)

	remaining := len(iters)

	for _, i := range iters {

		go func(l iterator.Iterator) {

			err := i.Append(ctx, c, append_funcs...)

			if err != nil {
				err_ch <- err
			}

			done_ch <- true

		}(i)
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
