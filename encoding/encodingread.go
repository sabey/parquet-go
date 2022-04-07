package encoding

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/sabey/parquet-go/parquet"
)

func ReadPlain(bytesReader *bytes.Reader, dataType parquet.Type, cnt uint64, bitWidth uint64) ([]interface{}, error) {
	if dataType == parquet.Type_BOOLEAN {
		v, err := ReadPlainBOOLEAN(bytesReader, cnt)
		if err != nil {
			return v, errors.Wrap(err, "ReadPlainBOOLEAN")
		}
		return v, nil
	} else if dataType == parquet.Type_INT32 {
		v, err := ReadPlainINT32(bytesReader, cnt)
		if err != nil {
			return v, errors.Wrap(err, "ReadPlainINT32")
		}
		return v, nil
	} else if dataType == parquet.Type_INT64 {
		v, err := ReadPlainINT64(bytesReader, cnt)
		if err != nil {
			return v, errors.Wrap(err, "ReadPlainINT64")
		}
		return v, nil
	} else if dataType == parquet.Type_INT96 {
		v, err := ReadPlainINT96(bytesReader, cnt)
		if err != nil {
			return v, errors.Wrap(err, "ReadPlainINT96")
		}
		return v, nil
	} else if dataType == parquet.Type_FLOAT {
		v, err := ReadPlainFLOAT(bytesReader, cnt)
		if err != nil {
			return v, errors.Wrap(err, "ReadPlainFLOAT")
		}
		return v, nil
	} else if dataType == parquet.Type_DOUBLE {
		v, err := ReadPlainDOUBLE(bytesReader, cnt)
		if err != nil {
			return v, errors.Wrap(err, "ReadPlainDOUBLE")
		}
		return v, nil
	} else if dataType == parquet.Type_BYTE_ARRAY {
		v, err := ReadPlainBYTE_ARRAY(bytesReader, cnt)
		if err != nil {
			return v, errors.Wrap(err, "ReadPlainBYTE_ARRAY")
		}
		return v, nil
	} else if dataType == parquet.Type_FIXED_LEN_BYTE_ARRAY {
		v, err := ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader, cnt, bitWidth)
		if err != nil {
			return v, errors.Wrap(err, "ReadPlainFIXED_LEN_BYTE_ARRAY")
		}
		return v, nil
	} else {
		return nil, errors.Errorf("Unknown parquet type")
	}
}

func ReadPlainBOOLEAN(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var (
		res []interface{}
		err error
	)

	res = make([]interface{}, cnt)
	resInt, err := ReadBitPacked(bytesReader, uint64(cnt<<1), 1)
	if err != nil {
		return res, errors.Wrap(err, "ReadBitPacked")
	}

	for i := 0; i < int(cnt); i++ {
		if resInt[i].(int64) > 0 {
			res[i] = true
		} else {
			res[i] = false
		}
	}
	return res, nil
}

func ReadPlainINT32(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadINT32(bytesReader, res)
	if err != nil {
		return res, errors.Wrap(err, "BinaryReadINT32")
	}
	return res, nil
}

func ReadPlainINT64(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadINT64(bytesReader, res)
	if err != nil {
		return res, errors.Wrap(err, "BinaryReadINT64")
	}
	return res, nil
}

func ReadPlainINT96(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	cur := make([]byte, 12)
	for i := 0; i < int(cnt); i++ {
		if _, err = bytesReader.Read(cur); err != nil {
			break
		}
		res[i] = string(cur[:12])
	}
	if err != nil {
		return res, errors.Wrap(err, "bytesReader.Read")
	}
	return res, nil
}

func ReadPlainFLOAT(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadFLOAT32(bytesReader, res)
	if err != nil {
		return res, errors.Wrap(err, "BinaryReadFLOAT32")
	}
	return res, nil
}

func ReadPlainDOUBLE(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	err = BinaryReadFLOAT64(bytesReader, res)
	if err != nil {
		return res, errors.Wrap(err, "BinaryReadFLOAT64")
	}
	return res, nil
}

func ReadPlainBYTE_ARRAY(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		buf := make([]byte, 4)
		if _, err = bytesReader.Read(buf); err != nil {
			break
		}
		ln := binary.LittleEndian.Uint32(buf)
		cur := make([]byte, ln)
		bytesReader.Read(cur)
		res[i] = string(cur)
	}
	if err != nil {
		return res, errors.Wrap(err, "bytesReader.Read")
	}
	return res, nil
}

func ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader *bytes.Reader, cnt uint64, fixedLength uint64) ([]interface{}, error) {
	var err error
	res := make([]interface{}, cnt)
	for i := 0; i < int(cnt); i++ {
		cur := make([]byte, fixedLength)
		if _, err = bytesReader.Read(cur); err != nil {
			break
		}
		res[i] = string(cur)
	}
	if err != nil {
		return res, errors.Wrap(err, "bytesReader.Read")
	}
	return res, nil
}

func ReadUnsignedVarInt(bytesReader *bytes.Reader) (uint64, error) {
	var err error
	var res uint64 = 0
	var shift uint64 = 0
	for {
		b, err := bytesReader.ReadByte()
		if err != nil {
			break
		}
		res |= ((uint64(b) & uint64(0x7F)) << uint64(shift))
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}
	if err != nil {
		return res, errors.Wrap(err, "bytesReader.ReadByte")
	}
	return res, nil
}

//RLE return res is []INT64
func ReadRLE(bytesReader *bytes.Reader, header uint64, bitWidth uint64) ([]interface{}, error) {
	var err error
	var res []interface{}
	cnt := header >> 1
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	if width > 0 {
		if _, err = bytesReader.Read(data); err != nil {
			return res, errors.Wrap(err, "bytesReader.Read")
		}
	}
	for len(data) < 4 {
		data = append(data, byte(0))
	}
	val := int64(binary.LittleEndian.Uint32(data))
	res = make([]interface{}, cnt)

	for i := 0; i < int(cnt); i++ {
		res[i] = val
	}
	return res, nil
}

//return res is []INT64
func ReadBitPacked(bytesReader *bytes.Reader, header uint64, bitWidth uint64) ([]interface{}, error) {
	var err error
	numGroup := (header >> 1)
	cnt := numGroup * 8
	byteCnt := cnt * bitWidth / 8

	res := make([]interface{}, 0, cnt)

	if cnt == 0 {
		return res, nil
	}

	if bitWidth == 0 {
		for i := 0; i < int(cnt); i++ {
			res = append(res, int64(0))
		}
		return res, nil
	}
	bytesBuf := make([]byte, byteCnt)
	if _, err = bytesReader.Read(bytesBuf); err != nil {
		return res, errors.Wrap(err, "bytesReader.Read")
	}

	i := 0
	var resCur uint64 = 0
	var resCurNeedBits uint64 = bitWidth
	var used uint64 = 0
	var left uint64 = 8 - used
	b := bytesBuf[i]
	for i < len(bytesBuf) {
		if left >= resCurNeedBits {
			resCur |= uint64(((uint64(b) >> uint64(used)) & ((1 << uint64(resCurNeedBits)) - 1)) << uint64(bitWidth-resCurNeedBits))
			res = append(res, int64(resCur))
			left -= resCurNeedBits
			used += resCurNeedBits

			resCurNeedBits = bitWidth
			resCur = 0

			if left <= 0 && i+1 < len(bytesBuf) {
				i += 1
				b = bytesBuf[i]
				left = 8
				used = 0
			}

		} else {
			resCur |= uint64((uint64(b) >> uint64(used)) << uint64(bitWidth-resCurNeedBits))
			i += 1
			if i < len(bytesBuf) {
				b = bytesBuf[i]
			}
			resCurNeedBits -= left
			left = 8
			used = 0
		}
	}
	return res, nil
}

//res is INT64
func ReadRLEBitPackedHybrid(bytesReader *bytes.Reader, bitWidth uint64, length uint64) ([]interface{}, error) {
	res := make([]interface{}, 0)
	if length <= 0 {
		lb, err := ReadPlainINT32(bytesReader, 1)
		if err != nil {
			return res, errors.Wrap(err, "ReadPlainINT32")
		}
		length = uint64(lb[0].(int32))
	}

	buf := make([]byte, length)
	if _, err := bytesReader.Read(buf); err != nil {
		return res, errors.Wrap(err, "bytesReader.Read")
	}

	newReader := bytes.NewReader(buf)
	for newReader.Len() > 0 {
		header, err := ReadUnsignedVarInt(newReader)
		if err != nil {
			return res, errors.Wrap(err, "ReadUnsignedVarInt")
		}
		if header&1 == 0 {
			buf, err := ReadRLE(newReader, header, bitWidth)
			if err != nil {
				return res, errors.Wrap(err, "ReadRLE")
			}
			res = append(res, buf...)

		} else {
			buf, err := ReadBitPacked(newReader, header, bitWidth)
			if err != nil {
				return res, errors.Wrap(err, "ReadBitPacked")
			}
			res = append(res, buf...)
		}
	}
	return res, nil
}

func ReadDeltaBinaryPackedINT32(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		err error
		res []interface{}
	)

	blockSize, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadUnsignedVarInt")
	}
	numMiniblocksInBlock, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadUnsignedVarInt")
	}
	numValues, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadUnsignedVarInt")
	}
	firstValueZigZag, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadUnsignedVarInt")
	}

	fv32 := int32(firstValueZigZag)
	var firstValue int32 = int32(uint32(fv32)>>1) ^ -(fv32 & 1)
	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res = make([]interface{}, 0)
	res = append(res, firstValue)
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, errors.Wrap(err, "ReadUnsignedVarInt")
		}

		md32 := int32(minDeltaZigZag)
		var minDelta int32 = int32(uint32(md32)>>1) ^ -(md32 & 1)
		var bitWidths = make([]uint64, numMiniblocksInBlock)
		for i := 0; uint64(i) < numMiniblocksInBlock; i++ {
			b, err := bytesReader.ReadByte()
			if err != nil {
				return res, errors.Wrap(err, "bytesReader.ReadByte")
			}
			bitWidths[i] = uint64(b)
		}
		for i := 0; uint64(i) < numMiniblocksInBlock && uint64(len(res)) < numValues; i++ {
			cur, err := ReadBitPacked(bytesReader, (numValuesInMiniBlock/8)<<1, bitWidths[i])
			if err != nil {
				return res, errors.Wrap(err, "ReadBitPacked")
			}
			for j := 0; j < len(cur) && len(res) < int(numValues); j++ {
				res = append(res, int32(res[len(res)-1].(int32)+int32(cur[j].(int64))+minDelta))
			}
		}
	}
	return res[:numValues], nil
}

//res is INT64
func ReadDeltaBinaryPackedINT64(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		err error
		res []interface{}
	)

	blockSize, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadUnsignedVarInt")
	}
	numMiniblocksInBlock, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadUnsignedVarInt")
	}
	numValues, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadUnsignedVarInt")
	}
	firstValueZigZag, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadUnsignedVarInt")
	}
	var firstValue int64 = int64(firstValueZigZag>>1) ^ -(int64(firstValueZigZag) & 1)

	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res = make([]interface{}, 0)
	res = append(res, int64(firstValue))
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, errors.Wrap(err, "ReadUnsignedVarInt")
		}
		var minDelta int64 = int64(minDeltaZigZag>>1) ^ -(int64(minDeltaZigZag) & 1)
		var bitWidths = make([]uint64, numMiniblocksInBlock)
		for i := 0; uint64(i) < numMiniblocksInBlock; i++ {
			b, err := bytesReader.ReadByte()
			if err != nil {
				return res, errors.Wrap(err, "bytesReader.ReadByte")
			}
			bitWidths[i] = uint64(b)
		}

		for i := 0; uint64(i) < numMiniblocksInBlock && uint64(len(res)) < numValues; i++ {
			cur, err := ReadBitPacked(bytesReader, (numValuesInMiniBlock/8)<<1, bitWidths[i])
			if err != nil {
				return res, errors.Wrap(err, "ReadBitPacked")
			}
			for j := 0; j < len(cur); j++ {
				res = append(res, (res[len(res)-1].(int64) + cur[j].(int64) + minDelta))
			}
		}
	}
	return res[:numValues], nil
}

func ReadDeltaLengthByteArray(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		res []interface{}
		err error
	)

	lengths, err := ReadDeltaBinaryPackedINT64(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadDeltaBinaryPackedINT64")
	}
	res = make([]interface{}, len(lengths))
	for i := 0; i < len(lengths); i++ {
		res[i] = ""
		length := uint64(lengths[i].(int64))
		if length > 0 {
			cur, err := ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader, 1, length)
			if err != nil {
				return res, errors.Wrap(err, "ReadPlainFIXED_LEN_BYTE_ARRAY")
			}
			res[i] = cur[0]
		}
	}

	return res, nil
}

func ReadDeltaByteArray(bytesReader *bytes.Reader) ([]interface{}, error) {
	var (
		res []interface{}
		err error
	)

	prefixLengths, err := ReadDeltaBinaryPackedINT64(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadDeltaBinaryPackedINT64")
	}
	suffixes, err := ReadDeltaLengthByteArray(bytesReader)
	if err != nil {
		return res, errors.Wrap(err, "ReadDeltaLengthByteArray")
	}
	res = make([]interface{}, len(prefixLengths))

	res[0] = suffixes[0]
	for i := 1; i < len(prefixLengths); i++ {
		prefixLength := prefixLengths[i].(int64)
		prefix := res[i-1].(string)[:prefixLength]
		suffix := suffixes[i].(string)
		res[i] = prefix + suffix
	}
	return res, nil
}

func ReadByteStreamSplitFloat32(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {

	res := make([]interface{}, cnt)
	buf := make([]byte, cnt*4)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return res, errors.Wrap(err, "io.ReadFull")
	}
	if cnt*4 != uint64(n) {
		return res, errors.Wrap(io.ErrUnexpectedEOF, "io.ErrUnexpectedEOF")
	}

	for i := uint64(0); i < cnt; i++ {
		res[i] = math.Float32frombits(uint32(buf[i]) |
			uint32(buf[cnt+i])<<8 |
			uint32(buf[cnt*2+i])<<16 |
			uint32(buf[cnt*3+i])<<24)
	}

	return res, nil
}

func ReadByteStreamSplitFloat64(bytesReader *bytes.Reader, cnt uint64) ([]interface{}, error) {

	res := make([]interface{}, cnt)
	buf := make([]byte, cnt*8)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return res, errors.Wrap(err, "io.ReadFull")
	}
	if cnt*8 != uint64(n) {
		return res, errors.Wrap(io.ErrUnexpectedEOF, "io.ErrUnexpectedEOF")
	}

	for i := uint64(0); i < cnt; i++ {
		res[i] = math.Float64frombits(uint64(buf[i]) |
			uint64(buf[cnt+i])<<8 |
			uint64(buf[cnt*2+i])<<16 |
			uint64(buf[cnt*3+i])<<24 |
			uint64(buf[cnt*4+i])<<32 |
			uint64(buf[cnt*5+i])<<40 |
			uint64(buf[cnt*6+i])<<48 |
			uint64(buf[cnt*7+i])<<56)
	}

	return res, nil
}
