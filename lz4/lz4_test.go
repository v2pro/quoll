package lz4

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func Test_VersionNumber(t *testing.T) {
	should := require.New(t)
	should.Equal(10701, VersionNumber())
}

func Test_CompressDefault(t *testing.T) {
	should := require.New(t)
	input := []byte{1, 2, 3}
	output := make([]byte, CompressBound(len(input)))
	compressedSize := CompressDefault(input, output)
	output = output[:compressedSize]
	input = make([]byte, len(input))
	should.Equal(len(input), DecompressSafe(output, input))
}
