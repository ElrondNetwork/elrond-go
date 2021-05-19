package mcl

import (
	"runtime"

	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/crypto"
	"github.com/herumi/bls-go-binary/bls"
)

var _ crypto.Scalar = (*Scalar)(nil)

// Scalar -
type Scalar struct {
	Scalar *bls.Fr
}

// NewScalar creates a scalar instance
func NewScalar() *Scalar {
	scalar := &Scalar{Scalar: &bls.Fr{}}
	scalar.Scalar.SetByCSPRNG()
	for scalar.Scalar.IsOne() || scalar.Scalar.IsZero() {
		scalar.Scalar.SetByCSPRNG()
	}

	return scalar
}

// Equal tests if receiver is equal with the scalarInt s given as parameter.
// Both scalars need to be derived from the same Group
func (sc *Scalar) Equal(s crypto.Scalar) (bool, error) {
	if check.IfNil(s) {
		return false, crypto.ErrNilParam
	}

	s2, ok := s.(*Scalar)
	if !ok {
		return false, crypto.ErrInvalidParam
	}

	areEqual := sc.Scalar.IsEqual(s2.Scalar)

	return areEqual, nil
}

// Set sets the receiver to Scalar s given as parameter
func (sc *Scalar) Set(s crypto.Scalar) error {
	if check.IfNil(s) {
		return crypto.ErrNilParam
	}

	s2, ok := s.(*Scalar)
	if !ok {
		return crypto.ErrInvalidParam
	}

	return sc.Scalar.Deserialize(s2.Scalar.Serialize())
}

// Clone creates a new Scalar with same value as receiver
func (sc *Scalar) Clone() crypto.Scalar {
	s := Scalar{
		Scalar: &bls.Fr{},
	}

	err := s.Scalar.Deserialize(sc.Scalar.Serialize())
	if err != nil {
		log.Error("MclScalar Clone", "error", err.Error())
	}

	return &s
}

// SetInt64 sets the receiver to a small integer value v given as parameter
func (sc *Scalar) SetInt64(v int64) {
	sc.Scalar.SetInt64(v)
}

// Zero returns the the additive identity (0)
func (sc *Scalar) Zero() crypto.Scalar {
	s := Scalar{
		Scalar: &bls.Fr{},
	}
	s.Scalar.Clear()

	return &s
}

// Add returns the modular sum of receiver with scalarInt s given as parameter
func (sc *Scalar) Add(s crypto.Scalar) (crypto.Scalar, error) {
	if check.IfNil(s) {
		return nil, crypto.ErrNilParam
	}

	s2, ok := s.(*Scalar)
	if !ok {
		return nil, crypto.ErrInvalidParam
	}

	s1 := Scalar{
		Scalar: &bls.Fr{},
	}

	bls.FrAdd(s1.Scalar, sc.Scalar, s2.Scalar)
	runtime.KeepAlive(s2)

	return &s1, nil
}

// Sub returns the modular difference between receiver and scalarInt s given as parameter
func (sc *Scalar) Sub(s crypto.Scalar) (crypto.Scalar, error) {
	if check.IfNil(s) {
		return nil, crypto.ErrNilParam
	}

	s2, ok := s.(*Scalar)
	if !ok {
		return nil, crypto.ErrInvalidParam
	}

	s1 := Scalar{
		Scalar: &bls.Fr{},
	}

	bls.FrSub(s1.Scalar, sc.Scalar, s2.Scalar)
	runtime.KeepAlive(s2)

	return &s1, nil
}

// Neg returns the modular negation of receiver
func (sc *Scalar) Neg() crypto.Scalar {
	s := Scalar{
		Scalar: &bls.Fr{},
	}

	bls.FrNeg(s.Scalar, sc.Scalar)

	return &s
}

// One sets the receiver to the multiplicative identity (1)
func (sc *Scalar) One() crypto.Scalar {
	s := Scalar{
		Scalar: &bls.Fr{},
	}

	s.Scalar.SetInt64(1)

	return &s
}

// Mul returns the modular product of receiver with scalarInt s given as parameter
func (sc *Scalar) Mul(s crypto.Scalar) (crypto.Scalar, error) {
	if check.IfNil(s) {
		return nil, crypto.ErrNilParam
	}

	s2, ok := s.(*Scalar)
	if !ok {
		return nil, crypto.ErrInvalidParam
	}

	s1 := Scalar{
		Scalar: &bls.Fr{},
	}

	bls.FrMul(s1.Scalar, sc.Scalar, s2.Scalar)
	runtime.KeepAlive(s2)

	return &s1, nil
}

// Div returns the modular division between receiver and scalarInt s given as parameter
func (sc *Scalar) Div(s crypto.Scalar) (crypto.Scalar, error) {
	if check.IfNil(s) {
		return nil, crypto.ErrNilParam
	}

	s2, ok := s.(*Scalar)
	if !ok {
		return nil, crypto.ErrInvalidParam
	}

	s1 := Scalar{
		Scalar: &bls.Fr{},
	}

	bls.FrDiv(s1.Scalar, sc.Scalar, s2.Scalar)
	runtime.KeepAlive(s2)

	return &s1, nil
}

// Inv returns the modular inverse of scalarInt s given as parameter
func (sc *Scalar) Inv(s crypto.Scalar) (crypto.Scalar, error) {
	if check.IfNil(s) {
		return nil, crypto.ErrNilParam
	}

	s2, ok := s.(*Scalar)
	if !ok {
		return nil, crypto.ErrInvalidParam
	}

	s1 := Scalar{
		Scalar: &bls.Fr{},
	}

	bls.FrInv(s1.Scalar, s2.Scalar)
	runtime.KeepAlive(s2)

	return &s1, nil
}

// Pick returns a fresh random or pseudo-random scalarInt
func (sc *Scalar) Pick() (crypto.Scalar, error) {
	s1 := Scalar{
		Scalar: &bls.Fr{},
	}

	s1.Scalar.SetByCSPRNG()

	for s1.Scalar.IsOne() || s1.Scalar.IsZero() {
		s1.Scalar.SetByCSPRNG()
	}

	return &s1, nil
}

// SetBytes sets the scalarInt from a byte-slice,
// reducing if necessary to the appropriate modulus.
func (sc *Scalar) SetBytes(s []byte) (crypto.Scalar, error) {
	if len(s) == 0 {
		return nil, crypto.ErrNilParam
	}

	s1 := sc.Clone()
	s1Mcl, ok := s1.(*Scalar)
	if !ok {
		return nil, crypto.ErrInvalidScalar
	}

	err := s1Mcl.Scalar.Deserialize(s)
	if err != nil {
		return nil, err
	}

	return s1, nil
}

// SetFromRandomBytes sets the scalar from a byte-slice of random bytes
func (sc *Scalar) SetFromRandomBytes(s []byte) (crypto.Scalar, error) {
	if len(s) == 0 {
		return nil, crypto.ErrNilParam
	}

	s1 := sc.Clone()
	s1Mcl, ok := s1.(*Scalar)
	if !ok {
		return nil, crypto.ErrInvalidScalar
	}

	result := s1Mcl.Scalar.SetHashOf(s)
	if result != true {
		return nil, crypto.ErrInvalidParam
	}

	return s1, nil
}

// GetUnderlyingObj returns the object the implementation wraps
func (sc *Scalar) GetUnderlyingObj() interface{} {
	return sc.Scalar
}

// MarshalBinary encodes the receiver into a binary form and returns the result.
func (sc *Scalar) MarshalBinary() ([]byte, error) {
	return sc.Scalar.Serialize(), nil
}

// UnmarshalBinary decodes a scalarInt from its byte array representation and sets the receiver to this value
func (sc *Scalar) UnmarshalBinary(s []byte) error {
	return sc.Scalar.Deserialize(s)
}

// IsInterfaceNil returns true if there is no value under the interface
func (sc *Scalar) IsInterfaceNil() bool {
	return sc == nil
}
