package mock

import "github.com/ElrondNetwork/elrond-go/data"

// HeaderSigVerifierStub -
type HeaderSigVerifierStub struct {
	VerifyLeaderSignatureCalled            func(header data.HeaderHandler) error
	VerifyRandSeedCalled                   func(header data.HeaderHandler) error
	VerifyRandSeedAndLeaderSignatureCalled func(header data.HeaderHandler) error
	VerifySignatureCalled                  func(header data.HeaderHandler) error
}

// VerifyRandSeed -
func (hsvm *HeaderSigVerifierStub) VerifyRandSeed(header data.HeaderHandler) error {
	if hsvm.VerifyRandSeedCalled != nil {
		return hsvm.VerifyRandSeedCalled(header)
	}

	return nil
}

// VerifyRandSeedAndLeaderSignature -
func (hsvm *HeaderSigVerifierStub) VerifyRandSeedAndLeaderSignature(header data.HeaderHandler) error {
	if hsvm.VerifyRandSeedAndLeaderSignatureCalled != nil {
		return hsvm.VerifyRandSeedAndLeaderSignatureCalled(header)
	}

	return nil
}

// VerifyLeaderSignature -
func (hsvm *HeaderSigVerifierStub) VerifyLeaderSignature(header data.HeaderHandler) error {
	if hsvm.VerifyLeaderSignatureCalled != nil {
		return hsvm.VerifyLeaderSignatureCalled(header)
	}

	return nil
}

// VerifySignature -
func (hsvm *HeaderSigVerifierStub) VerifySignature(header data.HeaderHandler) error {
	if hsvm.VerifySignatureCalled != nil {
		return hsvm.VerifySignatureCalled(header)
	}

	return nil
}

// IsInterfaceNil -
func (hsvm *HeaderSigVerifierStub) IsInterfaceNil() bool {
	return hsvm == nil
}
