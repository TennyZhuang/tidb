package tikv

import "github.com/pingcap/tidb/util/codec"

func mustDecode(k []byte) string {
	if len(k) == 0 {
		return ""
	}
	_, buf, err := codec.DecodeBytes([]byte(k), nil)
	if err != nil {
		return ""
	}

	return string(buf)
}
