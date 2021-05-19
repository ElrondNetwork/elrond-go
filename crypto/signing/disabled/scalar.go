package disabled

import "github.com/ElrondNetwork/elrond-go/crypto"

const marshaledScalar = "scalar"

type disabledScalar struct{}

// MarshalBinary returns a mock value
func (ds *disabledScalar) MarshalBinary() ([]byte, error) {
	return []byte(marshaledScalar), nil
}

// UnmarshalBinary returns nil
func (ds *disabledScalar) UnmarshalBinary(_ []byte) error {
	return nil
}

// Equal returns false
func (ds *disabledScalar) Equal(_ crypto.Scalar) (bool, error) {
	return false, nil
}

// Set returns nil
func (ds *disabledScalar) Set(_ crypto.Scalar) error {
	return nil
}

// Clone returns a new disabledScalar instance
func (ds *disabledScalar) Clone() crypto.Scalar {
	return &disabledScalar{}
}

// SetInt64 does nothing
func (ds *disabledScalar) SetInt64(_ int64) {
}

// Zero returns a new disabledScalar instance
func (ds *disabledScalar) Zero() crypto.Scalar {
	return &disabledScalar{}
}

// Add returns a new disabledScalar instance
func (ds *disabledScalar) Add(_ crypto.Scalar) (crypto.Scalar, error) {
	return &disabledScalar{}, nil
}

// Sub returns a new disabledScalar instance
func (ds *disabledScalar) Sub(_ crypto.Scalar) (crypto.Scalar, error) {
	return &disabledScalar{}, nil
}

// Neg returns a new disabledScalar instance
func (ds *disabledScalar) Neg() crypto.Scalar {
	return &disabledScalar{}
}

// One returns a new disabledScalar instance
func (ds *disabledScalar) One() crypto.Scalar {
	return &disabledScalar{}
}

// Mul returns a new disabledScalar instance
func (ds *disabledScalar) Mul(_ crypto.Scalar) (crypto.Scalar, error) {
	return &disabledScalar{}, nil
}

// Div returns a new disabledScalar instance
func (ds *disabledScalar) Div(_ crypto.Scalar) (crypto.Scalar, error) {
	return &disabledScalar{}, nil
}

// Inv returns a new disabledScalar instance
func (ds *disabledScalar) Inv(_ crypto.Scalar) (crypto.Scalar, error) {
	return &disabledScalar{}, nil
}

// Pick returns a new disabledScalar instance
func (ds *disabledScalar) Pick() (crypto.Scalar, error) {
	return &disabledScalar{}, nil
}

// SetBytes returns a new disabledScalar instance
func (ds *disabledScalar) SetBytes(_ []byte) (crypto.Scalar, error) {
	return &disabledScalar{}, nil
}

// SetFromRandomBytes returns a new disabledScalar instance
func (ds *disabledScalar) SetFromRandomBytes(_ []byte) (crypto.Scalar, error) {
	return &disabledScalar{}, nil
}

// GetUnderlyingObj returns nil
func (ds *disabledScalar) GetUnderlyingObj() interface{} {
	return nil
}

// IsInterfaceNil returns true if there is no value under the interface
func (ds *disabledScalar) IsInterfaceNil() bool {
	return ds == nil
}
