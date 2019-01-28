package sync

import (
	"bytes"
	"sync"

	"github.com/ElrondNetwork/elrond-go-sandbox/data/block"
	"github.com/pkg/errors"
)

type headerInfo struct {
	header     *block.Header
	hash       []byte
	isReceived bool
}

// BasicForkDetector defines a struct with necessary data needed for fork detection
type BasicForkDetector struct {
	headers    map[uint64][]*headerInfo
	mutHeaders sync.Mutex
}

// NewBasicForkDetector method creates a new BasicForkDetector object
func NewBasicForkDetector() *BasicForkDetector {
	bfd := &BasicForkDetector{}
	bfd.headers = make(map[uint64][]*headerInfo)

	return bfd
}

// AddHeader metrhod adds a new header to headers map
func (bfd *BasicForkDetector) AddHeader(header *block.Header, hash []byte, isReceived bool) error {
	if header == nil {
		return errors.New("nil header")
	}

	if hash == nil {
		return errors.New("nil hash")
	}

	if !isEmpty(header) && !isReceived {
		bfd.removePastHeaders(header.Nonce) // create a check point and remove all the past headers
	}

	bfd.append(&headerInfo{
		header:     header,
		hash:       hash,
		isReceived: isReceived,
	})

	return nil
}

func (bfd *BasicForkDetector) removePastHeaders(nonce uint64) {
	bfd.mutHeaders.Lock()

	for storedNonce := range bfd.headers {
		if storedNonce <= nonce {
			delete(bfd.headers, nonce)
		}
	}

	bfd.mutHeaders.Unlock()
}

// RemoveHeader method removes all the headers stored at the given nonce
func (bfd *BasicForkDetector) RemoveHeader(nonce uint64) {
	bfd.mutHeaders.Lock()
	delete(bfd.headers, nonce)
	bfd.mutHeaders.Unlock()
}

func (bfd *BasicForkDetector) append(hdrInfo *headerInfo) {
	bfd.mutHeaders.Lock()
	defer bfd.mutHeaders.Unlock()

	hdrInfos := bfd.headers[hdrInfo.header.Nonce]

	isHdrInfosNilOrEmpty := hdrInfos == nil || len(hdrInfos) == 0

	if isHdrInfosNilOrEmpty {
		bfd.headers[hdrInfo.header.Nonce] = []*headerInfo{hdrInfo}
		return
	}

	for _, hdrInfoStored := range bfd.headers[hdrInfo.header.Nonce] {
		if bytes.Equal(hdrInfoStored.hash, hdrInfo.hash) {
			return
		}
	}

	bfd.headers[hdrInfo.header.Nonce] = append(bfd.headers[hdrInfo.header.Nonce], hdrInfo)
}

// CheckFork metrhod checks if the node could be on the fork
func (bfd *BasicForkDetector) CheckFork() bool {
	bfd.mutHeaders.Lock()
	defer bfd.mutHeaders.Unlock()

	for nonce, hdrInfos := range bfd.headers {
		if len(hdrInfos) == 1 {
			continue
		}

		var selfHdrInfo *headerInfo
		foundNotEmptyBlock := false

		for i := 0; i < len(hdrInfos); i++ {
			if !hdrInfos[i].isReceived {
				selfHdrInfo = hdrInfos[i]
				continue
			}

			if !isEmpty(hdrInfos[i].header) {
				foundNotEmptyBlock = true
			}
		}

		if selfHdrInfo == nil {
			return false
		}

		if !isEmpty(selfHdrInfo.header) {
			delete(bfd.headers, nonce)
			bfd.headers[nonce] = []*headerInfo{selfHdrInfo}
		}

		return foundNotEmptyBlock
	}

	return false
}
