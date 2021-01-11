package compress

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	empty    = []byte("")
	value    = []byte("i-am-a-test-in")
	valueFmt = []byte{0xb, 0xa, 0xd, 0xa, 0x5, 0x5, 0x5, 0xb, 0x1e, 0x7a, 0x73, 0x74, 0x64, 0x1e,
		0x69, 0x2d, 0x61, 0x6d, 0x2d, 0x61, 0x2d, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x69, 0x6e}
	comp = []byte{
		0x28, 0xb5, 0x2f, 0xfd, 0x4, 0x0, 0x71, 0x0, 0x0, 0x69, 0x2d, 0x61, 0x6d, 0x2d, 0x61, 0x2d,
		0x74, 0x65, 0x73, 0x74, 0x2d, 0x69, 0x6e, 0x31, 0x49, 0x18, 0x48}
	compFmt = []byte{
		0xb, 0xa, 0xd, 0xa, 0x5, 0x5, 0x5, 0xb, 0x1e, 0x7a, 0x73, 0x74, 0x64, 0x1e, 0x28, 0xb5,
		0x2f, 0xfd, 0x4, 0x0, 0x71, 0x0, 0x0, 0x69, 0x2d, 0x61, 0x6d, 0x2d, 0x61, 0x2d, 0x74, 0x65,
		0x73, 0x74, 0x2d, 0x69, 0x6e, 0x31, 0x49, 0x18, 0x48}
	extra = []byte{
		0x69, 0x2d, 0x61, 0x6d, 0x2d, 0x1e, 0x2d, 0x74, 0x65, 0x73, 0x1e, 0x2d, 0x69, 0x6e}
	extraFmt = []byte{
		0xb, 0xa, 0xd, 0xa, 0x5, 0x5, 0x5, 0xb, 0x1e, 0x7a, 0x73, 0x74, 0x64, 0x1e, 0x69, 0x2d,
		0x61, 0x6d, 0x2d, 0x1e, 0x2d, 0x74, 0x65, 0x73, 0x1e, 0x2d, 0x69, 0x6e}
	badFmt = []byte{0xb, 0xa, 0xd, 0xa, 0x5, 0x5, 0x5, 0xb, 0x1e, 0xa, 0x73, 0x74, 0x64, 0x1e, 0x69,
		0x2d, 0x61, 0x6d, 0x2d, 0x61, 0x2d, 0x74, 0x65, 0x73, 0x74, 0x2d, 0x69, 0x6e}
)

func TestEncodeFormat(t *testing.T) {
	type testCase struct {
		in     []byte
		format Format
		out    []byte
	}
	var tests = []testCase{
		{nil, None, nil},
		{nil, Zstd, nil},
		{empty, None, empty},
		{empty, Zstd, empty},
		{value, None, value},
		{value, Zstd, valueFmt},
		{comp, Zstd, compFmt},
	}
	for _, test := range tests {
		out := EncodeFormat(test.in, test.format)
		assert.Equal(t, test.out, out)
	}
}

func TestDecodeFormat(t *testing.T) {
	type testCase struct {
		in     []byte
		out    []byte
		format Format
	}
	var tests = []testCase{
		{nil, nil, None},
		{empty, empty, None},
		{value, value, None},
		{valueFmt, value, Zstd},
		{compFmt, comp, Zstd},
		{extraFmt, extra, Zstd},
		{badFmt, badFmt, None},
	}
	for _, test := range tests {
		out, format := DecodeFormat(test.in)
		assert.Equal(t, test.format, format)
		assert.Equal(t, test.out, out)
	}
}
