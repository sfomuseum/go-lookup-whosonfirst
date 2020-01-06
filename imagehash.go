package whosonfirst

import (
	"context"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/sfomuseum/go-lookup"
	"io"
	"io/ioutil"
	_ "log"
)

func DefaultImageHashCatalogOptions() (*CatalogOptions, error) {

	opts, err := DefaultCatalogOptions()

	if err != nil {
		return nil, err
	}

	opts.AppendFuncs = append(opts.AppendFuncs, AppendImageHashFunc)

	return opts, nil
}

func AppendImageHashFunc(ctx context.Context, lu lookup.Catalog, fh io.ReadCloser) error {

	body, err := ioutil.ReadAll(fh)

	if err != nil {
		return err
	}

	if IsDeprecated(body) {
		return nil
	}

	id_rsp := gjson.GetBytes(body, "properties.wof:id")

	if !id_rsp.Exists() {
		// log.Println("MISSING ID")
		return nil
	}

	fp_rsp := gjson.GetBytes(body, "properties.media:imagehash_avg")

	if !fp_rsp.Exists() {
		// log.Println("MISSING IMAGE HASH", id_rsp.Int())
		return nil
	}

	fp := fp_rsp.String()
	id := id_rsp.Int()

	has_id, exists := lu.LoadOrStore(fp, id)

	if exists && id != has_id.(int64) {
		msg := fmt.Sprintf("Existing image hash key for %s (%d). Has ID: %d", fp, id, has_id.(int64))
		return errors.New(msg)
	}

	// log.Println(id, fp)
	return nil
}
