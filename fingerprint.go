package whosonfirst

import (
	"context"
	"errors"
	"fmt"
	"github.com/sfomuseum/go-lookup"
	"github.com/tidwall/gjson"
	"io"
	"io/ioutil"
	_ "log"
)

func DefaultFingerprintCatalogOptions() (*CatalogOptions, error) {

	opts, err := DefaultCatalogOptions()

	if err != nil {
		return nil, err
	}

	opts.AppendFuncs = append(opts.AppendFuncs, AppendFingerprintFunc)

	return opts, nil
}

func AppendFingerprintFunc(ctx context.Context, lu lookup.Catalog, fh io.ReadCloser) error {

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

	fp_rsp := gjson.GetBytes(body, "properties.media:fingerprint")

	if !fp_rsp.Exists() {
		// log.Println("MISSING FINGERPRINT")
		return nil
	}

	fp := fp_rsp.String()
	id := id_rsp.Int()

	has_id, exists := lu.LoadOrStore(fp, id)

	if exists && id != has_id.(int64) {
		msg := fmt.Sprintf("Existing fingerprint key for %s (%d). Has ID: %d", fp, id, has_id.(int64))
		return errors.New(msg)
	}

	// log.Println(id, fp)
	return nil
}
