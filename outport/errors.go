package outport

import "errors"

// ErrNilDriver signals that a nil driver has been provided
var ErrNilDriver = errors.New("nil driver")

// ErrNilArgsElasticDriverFactory signals that arguments that are needed for elastic driver factory are nil
var ErrNilArgsElasticDriverFactory = errors.New("nil args elastic driver factory")
