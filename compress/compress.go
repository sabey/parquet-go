package compress

import (
	"github.com/pkg/errors"
	"github.com/sabey/parquet-go/parquet"
)

type Compressor struct {
	Compress   func(buf []byte) []byte
	Uncompress func(buf []byte) ([]byte, error)
}

var compressors = map[parquet.CompressionCodec]*Compressor{}

func Uncompress(buf []byte, compressMethod parquet.CompressionCodec) ([]byte, error) {
	c, ok := compressors[compressMethod]
	if !ok {
		return nil, errors.Errorf("unsupported compress method")
	}

	bs, err := c.Uncompress(buf)
	if err != nil {
		return bs, errors.Wrap(err, "c.Uncompress")
	}
	return bs, nil
}

func Compress(buf []byte, compressMethod parquet.CompressionCodec) []byte {
	c, ok := compressors[compressMethod]
	if !ok {
		return nil
	}
	return c.Compress(buf)
}
