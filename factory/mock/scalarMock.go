package mock

import (
	"github.com/ElrondNetwork/elrond-go/crypto"
)

// ScalarMock represents a mock implementation of a Scalar
type ScalarMock struct {
	X int

	MarshalBinaryStub    func(x int) ([]byte, error)
	UnmarshalBinaryStub  func([]byte) (int, error)
	GetUnderlyingObjStub func() interface{}
}

// Equal tests if receiver is equal with the scalar s given as parameter.
// Both scalars need to be derived from the same Group
func (sm *ScalarMock) Equal(_ crypto.Scalar) (bool, error) {
	panic("implement me")
}

// Set sets the receiver to Scalar s given as parameter
func (sm *ScalarMock) Set(_ crypto.Scalar) error {
	panic("implement me")
}

// Clone creates a new Scalar with same value as receiver
func (sm *ScalarMock) Clone() crypto.Scalar {
	panic("implement me")
}

// SetInt64 sets the receiver to a small integer value v given as parameter
func (sm *ScalarMock) SetInt64(_ int64) {
	panic("implement me")
}

// Zero returns the the additive identity (0)
func (sm *ScalarMock) Zero() crypto.Scalar {
	panic("implement me")
}

// Add returns the modular sum of receiver with scalar s given as parameter
func (sm *ScalarMock) Add(_ crypto.Scalar) (crypto.Scalar, error) {
	panic("implement me")
}

// Sub returns the modular difference between receiver and scalar s given as parameter
func (sm *ScalarMock) Sub(_ crypto.Scalar) (crypto.Scalar, error) {
	panic("implement me")
}

// Neg returns the modular negation of receiver
func (sm *ScalarMock) Neg() crypto.Scalar {
	panic("implement me")
}

// One returns the multiplicative identity (1)
func (sm *ScalarMock) One() crypto.Scalar {
	panic("implement me")
}

// Mul returns the modular product of receiver with scalar s given as parameter
func (sm *ScalarMock) Mul(_ crypto.Scalar) (crypto.Scalar, error) {
	panic("implement me")
}

// Div returns the modular division between receiver and scalar s given as parameter
func (sm *ScalarMock) Div(_ crypto.Scalar) (crypto.Scalar, error) {
	panic("implement me")
}

// Inv returns the modular inverse of scalar s given as parameter
func (sm *ScalarMock) Inv(_ crypto.Scalar) (crypto.Scalar, error) {
	panic("implement me")
}

// Pick returns a fresh random or pseudo-random scalar
// For the mock set X to the original scalar.X *2
func (sm *ScalarMock) Pick() (crypto.Scalar, error) {
	ss := &ScalarMock{
		X:                   sm.X * 2,
		MarshalBinaryStub:   sm.MarshalBinaryStub,
		UnmarshalBinaryStub: sm.UnmarshalBinaryStub,
	}

	return ss, nil
}

// SetBytes sets the scalar from a byte-slice,
// reducing if necessary to the appropriate modulus.
func (sm *ScalarMock) SetBytes([]byte) (crypto.Scalar, error) {
	panic("implement me")
}

// SetFromRandomBytes sets the scalar from a byte-slice of random bytes
func (sm *ScalarMock) SetFromRandomBytes([]byte) (crypto.Scalar, error) {
	panic("implement me")
}

// GetUnderlyingObj returns the object the implementation wraps
func (sm *ScalarMock) GetUnderlyingObj() interface{} {
	if sm.GetUnderlyingObjStub != nil {
		return sm.GetUnderlyingObjStub()
	}
	return sm.X
}

// MarshalBinary transforms the Scalar into a byte array
func (sm *ScalarMock) MarshalBinary() ([]byte, error) {
	return sm.MarshalBinaryStub(sm.X)
}

// UnmarshalBinary recreates the Scalar from a byte array
func (sm *ScalarMock) UnmarshalBinary(val []byte) error {
	x, err := sm.UnmarshalBinaryStub(val)
	sm.X = x
	return err
}

// IsInterfaceNil returns true if there is no value under the interface
func (sm *ScalarMock) IsInterfaceNil() bool {
	return sm == nil
}
