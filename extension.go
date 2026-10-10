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

package zap

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"

	"go.uber.org/zap/zapcore"
)

// ownerPermanent marks factories installed through the permanent
// RegisterEncoder/RegisterSink APIs and the built-in "json", "console" and
// "file" defaults. Such entries have no scope and can never be revoked.
const ownerPermanent uint64 = 0

var errScopeReleased = errors.New("extension scope has already been released")

// extensionSet is one immutable version of the encoder and sink factory
// registries. Registries publish new versions atomically using
// copy-on-write, so any build that loads a snapshot observes a single,
// complete version: it can never see half of an in-progress installation.
type extensionSet struct {
	encoders     map[string]func(zapcore.EncoderConfig) (zapcore.Encoder, error)
	encoderOwner map[string]uint64
	sinks        map[string]func(*url.URL) (Sink, error)
	sinkOwner    map[string]uint64
}

func newExtensionSet() *extensionSet {
	return &extensionSet{
		encoders:     make(map[string]func(zapcore.EncoderConfig) (zapcore.Encoder, error)),
		encoderOwner: make(map[string]uint64),
		sinks:        make(map[string]func(*url.URL) (Sink, error)),
		sinkOwner:    make(map[string]uint64),
	}
}

func (s *extensionSet) clone() *extensionSet {
	next := newExtensionSet()
	for k, v := range s.encoders {
		next.encoders[k] = v
	}
	for k, v := range s.encoderOwner {
		next.encoderOwner[k] = v
	}
	for k, v := range s.sinks {
		next.sinks[k] = v
	}
	for k, v := range s.sinkOwner {
		next.sinkOwner[k] = v
	}
	return next
}

func (s *extensionSet) hasOwner(id uint64) bool {
	for _, owner := range s.encoderOwner {
		if owner == id {
			return true
		}
	}
	for _, owner := range s.sinkOwner {
		if owner == id {
			return true
		}
	}
	return false
}

func (s *extensionSet) newEncoder(name string, encoderConfig zapcore.EncoderConfig) (zapcore.Encoder, error) {
	if encoderConfig.TimeKey != "" && encoderConfig.EncodeTime == nil {
		return nil, errors.New("missing EncodeTime in EncoderConfig")
	}
	if name == "" {
		return nil, errNoEncoderNameSpecified
	}
	constructor, ok := s.encoders[name]
	if !ok {
		return nil, fmt.Errorf("no encoder registered for name %q", name)
	}
	return constructor(encoderConfig)
}

// extensionRegistry owns the current extensionSet and serializes
// copy-on-write publications. Reads are lock-free loads of the current
// snapshot; writes take mu so validate-then-publish is atomic across
// concurrent installs and removals.
type extensionRegistry struct {
	mu       sync.Mutex
	current  atomic.Pointer[extensionSet]
	openFile func(string, int, os.FileMode) (*os.File, error) // type matches os.OpenFile
	nextID   atomic.Uint64
}

func newExtensionRegistry() *extensionRegistry {
	r := &extensionRegistry{
		openFile: os.OpenFile,
	}
	r.current.Store(newExtensionSet())
	return r
}

func newDefaultExtensionRegistry() *extensionRegistry {
	r := newExtensionRegistry()
	// Infallible operations: the registry is empty, so there can't be a conflict.
	_ = r.registerEncoder("console", func(encoderConfig zapcore.EncoderConfig) (zapcore.Encoder, error) {
		return zapcore.NewConsoleEncoder(encoderConfig), nil
	})
	_ = r.registerEncoder("json", func(encoderConfig zapcore.EncoderConfig) (zapcore.Encoder, error) {
		return zapcore.NewJSONEncoder(encoderConfig), nil
	})
	_ = r.registerSink(schemeFile, r.newFileSinkFromURL)
	return r
}

func (r *extensionRegistry) snapshot() *extensionSet {
	return r.current.Load()
}

// mutate runs fn against a private copy of the current set and publishes the
// copy only when fn succeeds. A failure leaves the published set untouched.
func (r *extensionRegistry) mutate(fn func(*extensionSet) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	next := r.current.Load().clone()
	if err := fn(next); err != nil {
		return err
	}
	r.current.Store(next)
	return nil
}

// registerEncoder installs a permanent encoder factory, preserving the
// conflict semantics of the package-level RegisterEncoder.
func (r *extensionRegistry) registerEncoder(name string, constructor func(zapcore.EncoderConfig) (zapcore.Encoder, error)) error {
	if name == "" {
		return errNoEncoderNameSpecified
	}
	return r.mutate(func(s *extensionSet) error {
		if _, ok := s.encoders[name]; ok {
			return fmt.Errorf("encoder already registered for name %q", name)
		}
		s.encoders[name] = constructor
		s.encoderOwner[name] = ownerPermanent
		return nil
	})
}

// registerSink installs a permanent sink factory, preserving the validation
// and conflict semantics of the package-level RegisterSink.
func (r *extensionRegistry) registerSink(scheme string, factory func(*url.URL) (Sink, error)) error {
	if scheme == "" {
		return errors.New("can't register a sink factory for empty string")
	}
	normalized, err := normalizeScheme(scheme)
	if err != nil {
		return fmt.Errorf("%q is not a valid scheme: %v", scheme, err)
	}
	return r.mutate(func(s *extensionSet) error {
		if _, ok := s.sinks[normalized]; ok {
			return fmt.Errorf("sink factory already registered for scheme %q", normalized)
		}
		s.sinks[normalized] = factory
		s.sinkOwner[normalized] = ownerPermanent
		return nil
	})
}

// commit publishes a scope's staged factories as one atomic group. If any
// entry duplicates another entry in the batch or conflicts with an already
// published factory (owned by anyone), nothing is published and the previous
// state is left intact.
func (r *extensionRegistry) commit(id uint64, encoders []stagedEncoder, sinks []stagedSink) error {
	return r.mutate(func(s *extensionSet) error {
		seenEncoders := make(map[string]struct{}, len(encoders))
		for _, e := range encoders {
			if _, dup := seenEncoders[e.name]; dup {
				return fmt.Errorf("encoder already registered for name %q", e.name)
			}
			seenEncoders[e.name] = struct{}{}
			if _, ok := s.encoders[e.name]; ok {
				return fmt.Errorf("encoder already registered for name %q", e.name)
			}
		}

		seenSinks := make(map[string]struct{}, len(sinks))
		for _, sk := range sinks {
			if _, dup := seenSinks[sk.scheme]; dup {
				return fmt.Errorf("sink factory already registered for scheme %q", sk.scheme)
			}
			seenSinks[sk.scheme] = struct{}{}
			if _, ok := s.sinks[sk.scheme]; ok {
				return fmt.Errorf("sink factory already registered for scheme %q", sk.scheme)
			}
		}

		for _, e := range encoders {
			s.encoders[e.name] = e.constructor
			s.encoderOwner[e.name] = id
		}
		for _, sk := range sinks {
			s.sinks[sk.scheme] = sk.factory
			s.sinkOwner[sk.scheme] = id
		}
		return nil
	})
}

// release removes every factory owned by the scope in one atomic operation.
// It is a no-op when the scope owns nothing, which makes repeated releases
// idempotent.
func (r *extensionRegistry) release(id uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.current.Load().hasOwner(id) {
		return nil
	}

	next := r.current.Load().clone()
	for name, owner := range next.encoderOwner {
		if owner == id {
			delete(next.encoderOwner, name)
			delete(next.encoders, name)
		}
	}
	for scheme, owner := range next.sinkOwner {
		if owner == id {
			delete(next.sinkOwner, scheme)
			delete(next.sinks, scheme)
		}
	}
	r.current.Store(next)
	return nil
}

type stagedEncoder struct {
	name        string
	constructor func(zapcore.EncoderConfig) (zapcore.Encoder, error)
}

type stagedSink struct {
	scheme  string // normalized at staging time
	factory func(*url.URL) (Sink, error)
}

// ExtensionScope groups encoder and sink factories so they can be installed
// into and removed from the global registries as a single unit, for example
// when dynamically loading or unloading a logging extension.
//
// Factories staged with [ExtensionScope.RegisterEncoder] and
// [ExtensionScope.RegisterSink] stay invisible to [Config.Build] and [Open]
// until [ExtensionScope.Commit] publishes the whole group atomically.
// Concurrent builds therefore observe either the complete group or none of
// it, never a partially installed one. If any staged factory conflicts with
// an existing registration, Commit fails and the registries are left
// unchanged.
//
// [ExtensionScope.Release] revokes every factory owned by the scope in one
// atomic operation. Loggers, encoders and sinks built before the release
// keep working, as each build holds its own constructed instances and looks
// factories up from an immutable snapshot. Calling Release more than once,
// or on a scope that was never committed, is a no-op.
//
// Factories registered through the package-level [RegisterEncoder] and
// [RegisterSink] functions are permanent, coexist with scoped factories,
// and cannot be overridden or revoked.
//
// An ExtensionScope may be committed more than once; each commit publishes
// an additional atomic group, and a single Release revokes everything owned
// by the scope. ExtensionScope is safe for concurrent use.
type ExtensionScope struct {
	reg *extensionRegistry
	id  uint64

	mu       sync.Mutex
	released bool
	encoders []stagedEncoder
	sinks    []stagedSink
}

// NewExtensionScope starts a new, empty registration group bound to the
// global encoder and sink registries.
func NewExtensionScope() *ExtensionScope {
	return _extensions.newScope()
}

func (r *extensionRegistry) newScope() *ExtensionScope {
	return &ExtensionScope{
		reg: r,
		id:  r.nextID.Add(1),
	}
}

// RegisterEncoder stages an encoder constructor under the given name. The
// factory is not visible until [ExtensionScope.Commit] succeeds.
//
// An empty name is rejected immediately; name conflicts are reported by
// Commit.
func (s *ExtensionScope) RegisterEncoder(name string, constructor func(zapcore.EncoderConfig) (zapcore.Encoder, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.released {
		return errScopeReleased
	}
	if name == "" {
		return errNoEncoderNameSpecified
	}
	s.encoders = append(s.encoders, stagedEncoder{name: name, constructor: constructor})
	return nil
}

// RegisterSink stages a sink factory for the given scheme. The factory is not
// visible until [ExtensionScope.Commit] succeeds.
//
// The scheme follows the same rules as the package-level [RegisterSink];
// invalid schemes are rejected immediately, while scheme conflicts are
// reported by Commit.
func (s *ExtensionScope) RegisterSink(scheme string, factory func(*url.URL) (Sink, error)) error {
	if scheme == "" {
		return errors.New("can't register a sink factory for empty string")
	}
	normalized, err := normalizeScheme(scheme)
	if err != nil {
		return fmt.Errorf("%q is not a valid scheme: %v", scheme, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.released {
		return errScopeReleased
	}
	s.sinks = append(s.sinks, stagedSink{scheme: normalized, factory: factory})
	return nil
}

// Commit publishes all staged encoder and sink factories as one atomic
// group. If any staged factory duplicates another one in the same batch or
// conflicts with a factory that is already registered (permanently or by
// another active scope), Commit returns an error without modifying the
// registries, and the staged factories are retained so the call can be
// retried.
func (s *ExtensionScope) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.released {
		return errScopeReleased
	}
	if len(s.encoders) == 0 && len(s.sinks) == 0 {
		return nil
	}
	if err := s.reg.commit(s.id, s.encoders, s.sinks); err != nil {
		return err
	}
	s.encoders = nil
	s.sinks = nil
	return nil
}

// Release revokes all factories committed by the scope in one atomic
// operation. It is safe to call without a prior Commit and safe to call any
// number of times: every call after the first is a no-op.
func (s *ExtensionScope) Release() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.released {
		return nil
	}
	s.released = true
	s.encoders = nil
	s.sinks = nil
	return s.reg.release(s.id)
}
