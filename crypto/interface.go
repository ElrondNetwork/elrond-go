package crypto

import (
	"crypto/cipher"

	"github.com/ElrondNetwork/elrond-go/core"
)

// A Scalar represents a scalar value by which
// a Point (group element) may be encrypted to produce another Point.
type Scalar interface {
	// MarshalBinary transforms the Scalar into a byte array
	MarshalBinary() ([]byte, error)
	// UnmarshalBinary recreates the Scalar from a byte array
	UnmarshalBinary([]byte) error
	// Equal tests if receiver is equal with the scalar s given as parameter.
	// Both scalars need to be derived from the same Group
	Equal(s Scalar) (bool, error)
	// Set sets the receiver to Scalar s given as parameter
	Set(s Scalar) error
	// Clone creates a new Scalar with same value as receiver
	Clone() Scalar
	// SetInt64 sets the receiver to a small integer value v given as parameter
	SetInt64(v int64)
	// Zero returns the the additive identity (0)
	Zero() Scalar
	// Add returns the modular sum of receiver with scalar s given as parameter
	Add(s Scalar) (Scalar, error)
	// Sub returns the modular difference between receiver and scalar s given as parameter
	Sub(s Scalar) (Scalar, error)
	// Neg returns the modular negation of receiver
	Neg() Scalar
	// One returns the multiplicative identity (1)
	One() Scalar
	// Mul returns the modular product of receiver with scalar s given as parameter
	Mul(s Scalar) (Scalar, error)
	// Div returns the modular division between receiver and scalar s given as parameter
	Div(s Scalar) (Scalar, error)
	// Inv returns the modular inverse of scalar s given as parameter
	Inv(s Scalar) (Scalar, error)
	// Pick returns a fresh random or pseudo-random scalar
	Pick() (Scalar, error)
	// SetBytes sets the scalar from a byte-slice,
	// reducing if necessary to the appropriate modulus.
	SetBytes([]byte) (Scalar, error)
	// SetFromRandomBytes sets the scalar from a byte-slice of random bytes
	SetFromRandomBytes([]byte) (Scalar, error)
	// GetUnderlyingObj returns the object the implementation wraps
	GetUnderlyingObj() interface{}
	// IsInterfaceNil returns true if there is no value under the interface
	IsInterfaceNil() bool
}

// Point represents an element of a public-key cryptographic Group.
type Point interface {
	// MarshalBinary transforms the Point into a byte array
	MarshalBinary() ([]byte, error)
	// UnmarshalBinary recreates the Point from a byte array
	UnmarshalBinary([]byte) error
	// Equal tests if receiver is equal with the Point p given as parameter.
	// Both Points need to be derived from the same Group
	Equal(p Point) (bool, error)
	// Null returns the neutral identity element.
	Null() Point
	// Set sets the receiver equal to another Point p.
	Set(p Point) error
	// Clone returns a clone of the receiver.
	Clone() Point
	// Add returns the result of adding receiver with Point p given as parameter,
	// so that their scalars add homomorphically
	Add(p Point) (Point, error)
	// Sub returns the result of subtracting from receiver the Point p given as parameter,
	// so that their scalars subtract homomorphically
	Sub(p Point) (Point, error)
	// Neg returns the negation of receiver
	Neg() Point
	// Mul returns the result of multiplying receiver by the scalar s.
	Mul(s Scalar) (Point, error)
	// Pick returns a fresh random or pseudo-random Point.
	Pick() (Point, error)
	// GetUnderlyingObj returns the object the implementation wraps
	GetUnderlyingObj() interface{}
	// IsInterfaceNil returns true if there is no value under the interface
	IsInterfaceNil() bool
}

// Group defines a mathematical group used for Diffie-Hellmann operations
type Group interface {
	// String returns the string for the group
	String() string
	// ScalarLen returns the maximum length of scalars in bytes
	ScalarLen() int
	// CreateScalar creates a new Scalar
	CreateScalar() Scalar
	// PointLen returns the max length of point in nb of bytes
	PointLen() int
	// CreatePoint creates a new point
	CreatePoint() Point
	// CreatePointForScalar creates a new point corresponding to the given scalar
	CreatePointForScalar(scalar Scalar) (Point, error)
	// IsInterfaceNil returns true if there is no value under the interface
	IsInterfaceNil() bool
}

// Random is an interface that can be mixed in to local suite definitions.
type Random interface {
	// RandomStream returns a cipher.Stream that produces a
	// cryptographically random key stream. The stream must
	// tolerate being used in multiple goroutines.
	RandomStream() cipher.Stream
}

// Suite represents the list of functionalities needed by this package.
type Suite interface {
	Group
	Random
	// CreateKeyPair creates a scalar and a point pair that can be used in asymmetric cryptography
	CreateKeyPair() (Scalar, Point)
	// CheckPointValid returns nil if point is valid otherwise error. Zero is reported also as invalid
	CheckPointValid(pointBytes []byte) error
	// GetUnderlyingSuite returns the library suite that crypto.Suite wraps
	GetUnderlyingSuite() interface{}
}

// KeyGenerator is an interface for generating different types of cryptographic keys
type KeyGenerator interface {
	// GeneratePair creates a (crypto.PrivateKey, crypto.PublicKey) pair to be used for asymmetric cryptography
	GeneratePair() (PrivateKey, PublicKey)
	// PrivateKeyFromByteArray creates a crypto.PrivateKey from a byte array
	PrivateKeyFromByteArray(b []byte) (PrivateKey, error)
	//// PrivateKeyFromByteArrayRandom creates a crypto.PrivateKey from a randomly generated byte array
	//PrivateKeyFromByteArrayRandom(b []byte) (PrivateKey, error)
	// PublicKeyFromByteArray creates a crypto.PublicKey from a byte array
	PublicKeyFromByteArray(b []byte) (PublicKey, error)
	//CheckPublicKeyValid verifies the validity of the public key
	CheckPublicKeyValid(b []byte) error
	// Suite returns the crypto.Suite used by the KeyGenerator
	Suite() Suite
	// IsInterfaceNil returns true if there is no value under the interface
	IsInterfaceNil() bool
}

// Key represents a crypto key - can be either private or public
type Key interface {
	// ToByteArray returns the byte array representation of the key
	ToByteArray() ([]byte, error)
	// Suite returns the suite used by this key
	Suite() Suite
	// IsInterfaceNil returns true if there is no value under the interface
	IsInterfaceNil() bool
}

// PrivateKey represents a private key that can sign data or decrypt messages encrypted with a public key
type PrivateKey interface {
	Key
	// GeneratePublic builds a public key for the current private key
	GeneratePublic() PublicKey
	// Scalar returns the Scalar corresponding to this Private Key
	Scalar() Scalar
}

// PublicKey can be used to encrypt messages
type PublicKey interface {
	Key
	// Point returns the Point corresponding to this Public Key
	Point() Point
}

// SingleSigner provides functionality for signing a message and verifying a single signed message
type SingleSigner interface {
	// Sign is used to sign a message
	Sign(private PrivateKey, msg []byte) ([]byte, error)
	// Verify is used to verify a signed message
	Verify(public PublicKey, msg []byte, sig []byte) error
	// IsInterfaceNil returns true if there is no value under the interface
	IsInterfaceNil() bool
}

// MultiSigner provides functionality for multi-signing a message and verifying a multi-signed message
type MultiSigner interface {
	// MultiSigVerifier Provides functionality for verifying a multi-signature
	MultiSigVerifier
	// Reset resets the data holder inside the multiSigner
	Reset(pubKeys []string, index uint16) error
	// CreateSignatureShare creates a partial signature
	CreateSignatureShare(msg []byte, bitmap []byte) ([]byte, error)
	// StoreSignatureShare adds the partial signature of the signer with specified position
	StoreSignatureShare(index uint16, sig []byte) error
	// SignatureShare returns the partial signature set for given index
	SignatureShare(index uint16) ([]byte, error)
	// VerifySignatureShare verifies the partial signature of the signer with specified position
	VerifySignatureShare(index uint16, sig []byte, msg []byte, bitmap []byte) error
	// AggregateSigs aggregates all collected partial signatures
	AggregateSigs(bitmap []byte) ([]byte, error)
}

// MultiSigVerifier provides functionality for verifying a multi-signature
type MultiSigVerifier interface {
	// Create resets the multisigner and initializes to the new params
	Create(pubKeys []string, index uint16) (MultiSigner, error)
	// SetAggregatedSig sets the aggregated signature
	SetAggregatedSig([]byte) error
	// Verify verifies the aggregated signature
	Verify(msg []byte, bitmap []byte) error
	// IsInterfaceNil returns true if there is no value under the interface
	IsInterfaceNil() bool
}

// LowLevelSignerBLS provides functionality to sign and verify BLS single/multi-signatures
// Implementations act as a wrapper over a specific crypto library, such that changing the library requires only
// writing a new implementation of this LowLevelSigner
type LowLevelSignerBLS interface {
	// VerifySigShare verifies a BLS single signature
	VerifySigShare(pubKey PublicKey, message []byte, sig []byte) error
	// SignShare creates a BLS single signature over a given message
	SignShare(privKey PrivateKey, message []byte) ([]byte, error)
	// VerifySigBytes verifies if a byte array represents a BLS signature
	VerifySigBytes(suite Suite, sig []byte) error
	// AggregateSignatures aggregates BLS single signatures given as byte arrays
	AggregateSignatures(suite Suite, signatures [][]byte, pubKeysSigners []PublicKey) ([]byte, error)
	// VerifyAggregatedSig verifies the validity of an aggregated signature over a given message
	VerifyAggregatedSig(suite Suite, pubKeys []PublicKey, aggSigBytes []byte, msg []byte) error
}

// PeerSignatureHandler is a wrapper over SingleSigner that buffers the peer signatures.
// When it needs to sign or to verify a signature, it searches the buffer first.
type PeerSignatureHandler interface {
	VerifyPeerSignature(pk []byte, pid core.PeerID, signature []byte) error
	GetPeerSignature(key PrivateKey, pid []byte) ([]byte, error)
	IsInterfaceNil() bool
}
