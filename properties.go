package whosonfirst

import (
	"github.com/tidwall/gjson"
)

func IsDeprecated(body []byte) bool {

	deprecated_rsp := gjson.GetBytes(body, "edtf:deprecated")

	if !deprecated_rsp.Exists() {
		return false
	}

	return true
}
