package errs

import "errors"

var ErrNotFound = errors.New("Key not found")
var InvalidDataType = errors.New("INVALID DATA TYPE")
var IncorrectNumberOfArguments = errors.New("INCORRECT NUMBER OF ARGUMENTS")
var InvalidCommand = errors.New("INVALID COMMAND")
var InvalidMethod = errors.New("INVALID METHOD")
var TypeMismatch = errors.New("TYPE MISMATCH")