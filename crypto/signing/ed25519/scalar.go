package ed25519

import (
	"bytes"
	"crypto/ed25519"

	"github.com/ElrondNetwork/elrond-go/core/check"
	"github.com/ElrondNetwork/elrond-go/crypto"
)

var _ crypto.Scalar = (*ed25519Scalar)(nil)

type ed25519Scalar struct {
	ed25519.PrivateKey
}

// Equal checks if the underlying private key inside the scalar objects contain the same bytes
func (es *ed25519Scalar) Equal(s crypto.Scalar) (bool, error) {
	privateKey, err := es.getPrivateKeyFromScalar(s)
	if err != nil {
		return false, err
	}

	return bytes.Equal(privateKey, es.PrivateKey), nil
}

// Set sets the underlying private key inside the scalar to the private key of the provided scalar
func (es *ed25519Scalar) Set(s crypto.Scalar) error {
	privateKey, err := es.getPrivateKeyFromScalar(s)
	if err != nil {
		return err
	}

	es.PrivateKey = privateKey

	return nil
}

// Clone creates a new Scalar with same value as receiver
func (es *ed25519Scalar) Clone() crypto.Scalar {
	scalarBytes := make([]byte, len(es.PrivateKey))
	copy(scalarBytes, es.PrivateKey)

	return &ed25519Scalar{scalarBytes}
}

// GetUnderlyingObj returns the object the implementation wraps
func (es *ed25519Scalar) GetUnderlyingObj() interface{} {
	err := isKeyValid(es.PrivateKey)
	if err != nil {
		log.Error("ed25519Scalar",
			"message", "GetUnderlyingObj invalid private key construction")
		return nil
	}

	return es.PrivateKey
}

// MarshalBinary encodes the receiver into a binary form and returns the result.
func (es *ed25519Scalar) MarshalBinary() ([]byte, error) {
	err := isKeyValid(es.PrivateKey)
	if err != nil {
		return nil, err
	}

	return es.PrivateKey, nil
}

// UnmarshalBinary decodes a scalar from its byte array representation and sets the receiver to this value
func (es *ed25519Scalar) UnmarshalBinary(s []byte) error {
	switch len(s) {
	case ed25519.SeedSize:
		es.PrivateKey = ed25519.NewKeyFromSeed(s)
	case ed25519.PrivateKeySize:
		err := isKeyValid(s)
		if err != nil {
			return err
		}

		es.PrivateKey = s
	default:
		return crypto.ErrInvalidPrivateKey
	}

	return nil
}

// SetInt64 is not needed for this use case, should be removed if possible
func (es *ed25519Scalar) SetInt64(_ int64) {
	log.Error("ed25519Scalar",
		"message", "SetInt64 for ed25519Scalar is not implemented, should not be called")
}

// Zero is not needed for this use case, should be removed if possible
func (es *ed25519Scalar) Zero() crypto.Scalar {
	log.Error("ed25519Scalar",
		"message", "SetInt64 for ed25519Scalar is not implemented, should not be called")

	return nil
}

// Add is not needed for this use case, should be removed if possible
func (es *ed25519Scalar) Add(_ crypto.Scalar) (crypto.Scalar, error) {
	log.Error("ed25519Scalar",
		"message", "Add for ed25519Scalar is not implemented, should not be called")

	return nil, crypto.ErrNotImplemented
}

// Sub is not needed for this use case, should be removed if possible
func (es *ed25519Scalar) Sub(_ crypto.Scalar) (crypto.Scalar, error) {
	log.Error("ed25519Scalar",
		"message", "Sub for ed25519Scalar is not implemented, should not be called")

	return nil, crypto.ErrNotImplemented
}

// Neg is not needed for this use case, should be removed if possible
func (es *ed25519Scalar) Neg() crypto.Scalar {
	log.Error("ed25519Scalar",
		"message", "Neg for ed25519Scalar is not implemented, should not be called")

	return nil
}

// One is not needed for this use case, should be removed if possible
func (es *ed25519Scalar) One() crypto.Scalar {
	log.Error("ed25519Scalar",
		"message", "One for ed25519Scalar is not implemented, should not be called")

	return nil
}

// Mul returns the modular product of receiver with scalar s given as parameter
func (es *ed25519Scalar) Mul(_ crypto.Scalar) (crypto.Scalar, error) {
	log.Error("ed25519Scalar",
		"message", "Mul for ed25519Scalar is not implemented, should not be called")

	return nil, crypto.ErrNotImplemented
}

// Div returns the modular division between receiver and scalar s given as parameter
func (es *ed25519Scalar) Div(_ crypto.Scalar) (crypto.Scalar, error) {
	log.Error("ed25519Scalar",
		"message", "Div for ed25519Scalar is not implemented, should not be called")

	return nil, crypto.ErrNotImplemented
}

// Inv returns the modular inverse of scalar s given as parameter
func (es *ed25519Scalar) Inv(_ crypto.Scalar) (crypto.Scalar, error) {
	log.Error("ed25519Scalar",
		"message", "Inv for ed25519Scalar is not implemented, should not be called")

	return nil, crypto.ErrNotImplemented
}

// Pick returns a fresh random or pseudo-random scalar
func (es *ed25519Scalar) Pick() (crypto.Scalar, error) {
	log.Error("ed25519Scalar",
		"message", "Pick for ed25519Scalar is not implemented, should not be called")

	return nil, crypto.ErrNotImplemented
}

// SetBytes sets the scalar from a byte-slice,
// reducing if necessary to the appropriate modulus.
func (es *ed25519Scalar) SetBytes(_ []byte) (crypto.Scalar, error) {
	log.Error("ed25519Scalar",
		"message", "SetBytes for ed25519Scalar is not implemented, should not be called")

	return nil, crypto.ErrNotImplemented
}

// SetFromRandomBytes sets the scalar from a byte-slice of random bytes
func (es *ed25519Scalar) SetFromRandomBytes(_ []byte) (crypto.Scalar, error) {
	log.Error("ed25519Scalar",
		"message", "SetFromRandomBytes for ed25519Scalar is not implemented, should not be called")

	return nil, crypto.ErrNotImplemented
}

// IsInterfaceNil returns true if there is no value under the interface
func (es *ed25519Scalar) IsInterfaceNil() bool {
	return es == nil
}

func (es *ed25519Scalar) getPrivateKeyFromScalar(s crypto.Scalar) (ed25519.PrivateKey, error) {
	if check.IfNil(s) {
		return nil, crypto.ErrNilParam
	}

	privateKey, ok := s.GetUnderlyingObj().(ed25519.PrivateKey)
	if !ok {
		return nil, crypto.ErrInvalidPrivateKey
	}

	return privateKey, nil
}

func isKeyValid(key ed25519.PrivateKey) error {
	if len(key) != ed25519.PrivateKeySize {
		return crypto.ErrWrongPrivateKeySize
	}

	seed := key.Seed()
	validationKey := ed25519.NewKeyFromSeed(seed)
	if !bytes.Equal(key, validationKey) {
		return crypto.ErrWrongPrivateKeyStructure
	}

	return nil
}
