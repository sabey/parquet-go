//go:build !no_snappy
// +build !no_snappy

package compress

import (
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/sabey/parquet-go/parquet"
)

func init() {
	compressors[parquet.CompressionCodec_SNAPPY] = &Compressor{
		Compress: func(buf []byte) []byte {
			return snappy.Encode(nil, buf)
		},
		Uncompress: func(buf []byte) (bytes []byte, err error) {
			bs, err := snappy.Decode(nil, buf)
			if err != nil {
				return bs, errors.Wrap(err, "snappy.Decode")
			}

			return bs, nil
		},
	}
}
