package mock

import (
	"encoding/hex"
)

// PubkeyConverterMock -
type PubkeyConverterMock struct {
	len int
}

// NewPubkeyConverterMock -
func NewPubkeyConverterMock(addressLen int) *PubkeyConverterMock {
	return &PubkeyConverterMock{
		len: addressLen,
	}
}

// Decode -
func (pcm *PubkeyConverterMock) Decode(humanReadable string) ([]byte, error) {
	return hex.DecodeString(humanReadable)
}

// Encode -
func (pcm *PubkeyConverterMock) Encode(pkBytes []byte) string {
	return hex.EncodeToString(pkBytes)
}

// Len -
func (pcm *PubkeyConverterMock) Len() int {
	return pcm.len
}

// IsInterfaceNil -
func (pcm *PubkeyConverterMock) IsInterfaceNil() bool {
	return pcm == nil
}
