// Copyright (c) 2026 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zapcore

import (
	"errors"
	"fmt"
)

// ErrFieldLimitExceeded is returned by an Encoder when a log entry is dropped
// because it exceeds the per-entry limits on business fields or nesting depth
// configured via EncoderConfig.MaxFields or EncoderConfig.MaxDepth.
var ErrFieldLimitExceeded = errors.New("field limit exceeded")

// FieldLimitReason identifies which limit caused an entry to be rejected.
type FieldLimitReason uint8

const (
	// FieldLimitFieldsExceeded indicates that the entry contained more
	// business fields than allowed by EncoderConfig.MaxFields.
	FieldLimitFieldsExceeded FieldLimitReason = iota + 1
	// FieldLimitDepthExceeded indicates that the entry was nested more
	// deeply than allowed by EncoderConfig.MaxDepth.
	FieldLimitDepthExceeded
)

// FieldLimitError describes a log entry that was rejected because it exceeded
// the field-count or nesting-depth limits configured on the EncoderConfig.
//
// The whole entry is dropped: nothing is written to the encoder's output, and
// the error is returned from Encoder.EncodeEntry (and ultimately from
// Core.Write). If EncoderConfig.OnFieldLimit is set, it is invoked with the
// same error before EncodeEntry returns.
type FieldLimitError struct {
	// Reason is the limit that was exceeded.
	Reason FieldLimitReason
	// Limit is the configured limit (MaxFields or MaxDepth).
	Limit int
	// Fields is the number of business fields counted when the entry was
	// rejected.
	Fields int
	// Depth is the nesting depth reached when the entry was rejected.
	Depth int
	// Key is the key of the field that was being added when the limit was
	// exceeded. It may be empty, for example when the limit was exceeded
	// inside reflected content.
	Key string
}

// Error implements the error interface.
func (e *FieldLimitError) Error() string {
	switch e.Reason {
	case FieldLimitFieldsExceeded:
		return fmt.Sprintf(
			"field limit exceeded: entry contains %d business fields, limit is %d",
			e.Fields, e.Limit,
		)
	case FieldLimitDepthExceeded:
		return fmt.Sprintf(
			"field limit exceeded: entry reaches nesting depth %d, limit is %d",
			e.Depth, e.Limit,
		)
	default:
		return ErrFieldLimitExceeded.Error()
	}
}

// Unwrap allows errors.Is checks against ErrFieldLimitExceeded.
func (e *FieldLimitError) Unwrap() error {
	return ErrFieldLimitExceeded
}
