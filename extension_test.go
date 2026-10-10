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
	"bytes"
	"io"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap/zapcore"
)

// stubExtensions replaces both global registries with fresh ones that have
// only the built-in defaults, and restores them when the test finishes.
func stubExtensions(t *testing.T) *extensionRegistry {
	t.Helper()

	prevExtensions := _extensions
	prevSinkRegistry := _sinkRegistry
	t.Cleanup(func() {
		_extensions = prevExtensions
		_sinkRegistry = prevSinkRegistry
	})

	r := newDefaultExtensionRegistry()
	_extensions = r
	_sinkRegistry = newSinkRegistryFrom(r)
	return r
}

func groupedEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey: "msg",
		LineEnding: zapcore.DefaultLineEnding,
	}
}

func jsonEncoderConstructor(cfg zapcore.EncoderConfig) (zapcore.Encoder, error) {
	return zapcore.NewJSONEncoder(cfg), nil
}

// bufferSinkFactory returns a sink factory that records every write in the
// returned buffer.
func bufferSinkFactory() (*bytes.Buffer, func(*url.URL) (Sink, error)) {
	buf := bytes.NewBuffer(nil)
	return buf, func(*url.URL) (Sink, error) {
		return nopCloserSink{zapcore.AddSync(buf)}, nil
	}
}

func buildGroupedLogger(t *testing.T, encoding string, paths ...string) *Logger {
	t.Helper()

	cfg := Config{
		Level:            NewAtomicLevelAt(InfoLevel),
		Encoding:         encoding,
		EncoderConfig:    groupedEncoderConfig(),
		OutputPaths:      paths,
		ErrorOutputPaths: []string{"stderr"},
	}
	log, err := cfg.Build()
	require.NoError(t, err)
	return log
}

// A whole group (encoder + sink) is invisible before Commit and fully usable
// after it.
func TestExtensionScopeCommitPublishesGroup(t *testing.T) {
	stubExtensions(t)

	buf, sinkFactory := bufferSinkFactory()
	scope := NewExtensionScope()
	require.NoError(t, scope.RegisterEncoder("grouped-json", jsonEncoderConstructor))
	require.NoError(t, scope.RegisterSink("grouped-mem", sinkFactory))

	// Before Commit, neither factory is visible.
	snap := _extensions.snapshot()
	_, encOK := snap.encoders["grouped-json"]
	_, sinkOK := snap.sinks["grouped-mem"]
	assert.False(t, encOK, "encoder must not be visible before Commit")
	assert.False(t, sinkOK, "sink must not be visible before Commit")

	require.NoError(t, scope.Commit())

	// After Commit, the complete group is visible and a logger can be built
	// end-to-end through both factories.
	log := buildGroupedLogger(t, "grouped-json", "grouped-mem://x")
	log.Info("hello")
	require.NoError(t, log.Sync())
	assert.Contains(t, buf.String(), `"msg":"hello"`)
}

// Any conflict in the batch rejects the whole group and leaves the previous
// registry state untouched.
func TestExtensionScopeCommitConflictRollsBack(t *testing.T) {
	r := stubExtensions(t)

	t.Run("conflict with permanent factory", func(t *testing.T) {
		_, sinkFactory := bufferSinkFactory()
		scope := NewExtensionScope()
		require.NoError(t, scope.RegisterEncoder("rolled-back", jsonEncoderConstructor))
		require.NoError(t, scope.RegisterSink(schemeFile, sinkFactory)) // permanent conflict

		err := scope.Commit()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sink factory already registered for scheme \"file\"")

		// The failed group is not visible and the permanent default is intact.
		snap := r.snapshot()
		_, encOK := snap.encoders["rolled-back"]
		assert.False(t, encOK, "encoder must not be published after a failed group commit")
		assert.NotNil(t, snap.sinks[schemeFile], "permanent file sink must remain registered")

		// The defaults still build and log without error. (Don't Sync:
		// some test environments don't support syncing stderr.)
		log := buildGroupedLogger(t, "json", "stderr")
		log.Info("still here")
	})

	t.Run("duplicate inside the same batch", func(t *testing.T) {
		_, sinkFactory := bufferSinkFactory()
		scope := NewExtensionScope()
		require.NoError(t, scope.RegisterEncoder("dup", jsonEncoderConstructor))
		require.NoError(t, scope.RegisterEncoder("dup", jsonEncoderConstructor))
		require.NoError(t, scope.RegisterSink("dup-mem", sinkFactory))

		require.Error(t, scope.Commit())

		snap := r.snapshot()
		_, encOK := snap.encoders["dup"]
		_, sinkOK := snap.sinks["dup-mem"]
		assert.False(t, encOK, "nothing from a conflicting batch may be published")
		assert.False(t, sinkOK)
	})

	t.Run("retry after the external conflict is removed", func(t *testing.T) {
		holder := NewExtensionScope()
		require.NoError(t, holder.RegisterEncoder("ext", jsonEncoderConstructor))
		require.NoError(t, holder.Commit())

		// Staged factories survive the failed commit so the batch can be
		// retried once the blocking registration goes away.
		scope := NewExtensionScope()
		require.NoError(t, scope.RegisterEncoder("ext", jsonEncoderConstructor))
		require.Error(t, scope.Commit())

		require.NoError(t, holder.Release())
		require.NoError(t, scope.Commit())

		_, err := newEncoder("ext", groupedEncoderConfig())
		require.NoError(t, err)
	})
}

// A failed second commit must not disturb an already published first commit.
func TestExtensionScopeFailedCommitKeepsPriorState(t *testing.T) {
	stubExtensions(t)

	scope := NewExtensionScope()
	require.NoError(t, scope.RegisterEncoder("first", jsonEncoderConstructor))
	require.NoError(t, scope.Commit())

	require.NoError(t, scope.RegisterEncoder("second", jsonEncoderConstructor))
	require.NoError(t, scope.RegisterSink(schemeFile, func(*url.URL) (Sink, error) {
		return nopCloserSink{zapcore.AddSync(io.Discard)}, nil
	}))
	require.Error(t, scope.Commit())

	// The first group stays usable; the failed second group stays invisible.
	log := buildGroupedLogger(t, "first", "stderr")
	log.Info("one")

	_, err := newEncoder("second", groupedEncoderConfig())
	require.Error(t, err)
}

// Release revokes the whole group, is idempotent, and frees the names for
// reuse.
func TestExtensionScopeReleaseIdempotent(t *testing.T) {
	stubExtensions(t)

	_, sinkFactory := bufferSinkFactory()
	scope := NewExtensionScope()
	require.NoError(t, scope.RegisterEncoder("revokable", jsonEncoderConstructor))
	require.NoError(t, scope.RegisterSink("revokable-mem", sinkFactory))
	require.NoError(t, scope.Commit())

	require.NoError(t, scope.Release())
	require.NoError(t, scope.Release(), "repeated Release must be a no-op")

	_, err := newEncoder("revokable", groupedEncoderConfig())
	require.Error(t, err)
	_, _, err = Open("revokable-mem://x")
	require.Error(t, err)

	// Names freed by Release can be re-installed by a fresh scope.
	reloaded := NewExtensionScope()
	require.NoError(t, reloaded.RegisterEncoder("revokable", jsonEncoderConstructor))
	require.NoError(t, reloaded.RegisterSink("revokable-mem", sinkFactory))
	require.NoError(t, reloaded.Commit())
	require.NoError(t, reloaded.Release())
	require.NoError(t, reloaded.Release())
}

// Multiple commits accumulate under one scope; a single Release removes all
// of them.
func TestExtensionScopeMultipleCommitUnionRelease(t *testing.T) {
	stubExtensions(t)

	scope := NewExtensionScope()
	require.NoError(t, scope.RegisterEncoder("a", jsonEncoderConstructor))
	require.NoError(t, scope.Commit())
	require.NoError(t, scope.RegisterEncoder("b", jsonEncoderConstructor))
	require.NoError(t, scope.RegisterSink("b-mem", func(*url.URL) (Sink, error) {
		return nopCloserSink{zapcore.AddSync(io.Discard)}, nil
	}))
	require.NoError(t, scope.Commit())

	snap := _extensions.snapshot()
	assert.Len(t, snap.encoders, 4) // json, console, a, b
	assert.Len(t, snap.sinks, 2)    // file, b-mem

	// Re-committing an already-owned name conflicts.
	require.NoError(t, scope.RegisterEncoder("a", jsonEncoderConstructor))
	require.Error(t, scope.Commit())

	require.NoError(t, scope.Release())

	snap = _extensions.snapshot()
	assert.Len(t, snap.encoders, 2, "all scoped encoders must be gone")
	assert.Len(t, snap.sinks, 1, "all scoped sinks must be gone")
	assert.NotNil(t, snap.encoders["json"])
	assert.NotNil(t, snap.sinks[schemeFile])
}

// Instances built before a release keep working after it.
func TestExtensionScopeInstancesSurviveRelease(t *testing.T) {
	stubExtensions(t)

	buf, sinkFactory := bufferSinkFactory()
	scope := NewExtensionScope()
	require.NoError(t, scope.RegisterEncoder("survivor", jsonEncoderConstructor))
	require.NoError(t, scope.RegisterSink("survivor-mem", sinkFactory))
	require.NoError(t, scope.Commit())

	log := buildGroupedLogger(t, "survivor", "survivor-mem://x")
	log.Info("before")
	require.NoError(t, log.Sync())

	require.NoError(t, scope.Release())

	// New builds can no longer resolve the group...
	_, err := Config{
		Level:            NewAtomicLevelAt(InfoLevel),
		Encoding:         "survivor",
		EncoderConfig:    groupedEncoderConfig(),
		OutputPaths:      []string{"survivor-mem://x"},
		ErrorOutputPaths: []string{"stderr"},
	}.Build()
	require.Error(t, err)

	// ...but the logger and the sink it already holds are unaffected.
	before := buf.Len()
	log.Info("after")
	require.NoError(t, log.Sync())
	assert.Greater(t, buf.Len(), before)
	assert.Contains(t, buf.String(), `"msg":"after"`)
}

// Permanent registrations and the built-in defaults keep their exact
// semantics and coexist with, but cannot be overridden by, scoped groups.
func TestExtensionScopePermanentCompatibility(t *testing.T) {
	stubExtensions(t)

	t.Run("scope cannot override permanent encoder", func(t *testing.T) {
		scope := NewExtensionScope()
		require.NoError(t, scope.RegisterEncoder("json", jsonEncoderConstructor))
		err := scope.Commit()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "encoder already registered for name \"json\"")
	})

	t.Run("scope cannot override permanent sink", func(t *testing.T) {
		scope := NewExtensionScope()
		require.NoError(t, scope.RegisterSink("FILE", func(*url.URL) (Sink, error) {
			return nopCloserSink{zapcore.AddSync(io.Discard)}, nil
		}))
		err := scope.Commit()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "sink factory already registered for scheme \"file\"")
	})

	t.Run("permanent cannot override active scope until release", func(t *testing.T) {
		scope := NewExtensionScope()
		require.NoError(t, scope.RegisterEncoder("perm-x", jsonEncoderConstructor))
		require.NoError(t, scope.RegisterSink("perm-s", func(*url.URL) (Sink, error) {
			return nopCloserSink{zapcore.AddSync(io.Discard)}, nil
		}))
		require.NoError(t, scope.Commit())

		require.Error(t, RegisterEncoder("perm-x", jsonEncoderConstructor))
		require.Error(t, RegisterSink("PERM-S", func(*url.URL) (Sink, error) {
			return nopCloserSink{zapcore.AddSync(io.Discard)}, nil
		}))

		require.NoError(t, scope.Release())

		require.NoError(t, RegisterEncoder("perm-x", jsonEncoderConstructor))
		require.NoError(t, RegisterSink("perm-s", func(*url.URL) (Sink, error) {
			return nopCloserSink{zapcore.AddSync(io.Discard)}, nil
		}))
	})
}

// Staging-time validation and the released-state rules.
func TestExtensionScopeStagingValidation(t *testing.T) {
	stubExtensions(t)

	scope := NewExtensionScope()
	assert.Equal(t, errNoEncoderNameSpecified, scope.RegisterEncoder("", jsonEncoderConstructor))

	err := scope.RegisterSink("", func(*url.URL) (Sink, error) { return nil, nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty string")

	err = scope.RegisterSink("42", func(*url.URL) (Sink, error) { return nil, nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a valid scheme")

	// Empty commit and release-without-commit are harmless.
	require.NoError(t, scope.Commit())
	require.NoError(t, scope.Release())
	require.NoError(t, scope.Release())

	assert.Equal(t, errScopeReleased, scope.RegisterEncoder("late", jsonEncoderConstructor))
	err = scope.RegisterSink("late", func(*url.URL) (Sink, error) { return nil, nil })
	assert.Equal(t, errScopeReleased, err)
	assert.Equal(t, errScopeReleased, scope.Commit())
}

// Concurrent with commit/release churn, every snapshot and every build
// observes the group as a whole: both encoder and sink present, or neither.
func TestExtensionScopeAtomicVisibilityConcurrent(t *testing.T) {
	stubExtensions(t)

	const iterations = 2000

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Single mutator: install the full group, then revoke it, repeatedly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			scope := NewExtensionScope()
			if err := scope.RegisterEncoder("dyn", jsonEncoderConstructor); err != nil {
				t.Errorf("stage encoder: %v", err)
				return
			}
			if err := scope.RegisterSink("dyn", func(*url.URL) (Sink, error) {
				return nopCloserSink{zapcore.AddSync(io.Discard)}, nil
			}); err != nil {
				t.Errorf("stage sink: %v", err)
				return
			}
			if err := scope.Commit(); err != nil {
				t.Errorf("commit: %v", err)
				return
			}
			if err := scope.Release(); err != nil {
				t.Errorf("release: %v", err)
				return
			}
		}
		stop.Store(true)
	}()

	// Snapshot readers: the encoder and sink of one group always appear
	// together in the same immutable version.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				snap := _extensions.snapshot()
				_, encOK := snap.encoders["dyn"]
				_, sinkOK := snap.sinks["dyn"]
				if encOK != sinkOK {
					t.Errorf("partial group visible: encoder=%v sink=%v", encOK, sinkOK)
					return
				}
			}
		}()
	}

	// Full builds: they either succeed using the complete group, or fail
	// cleanly because the group is absent. A half-installed group must never
	// produce any other error.
	cfg := Config{
		Level:            NewAtomicLevelAt(InfoLevel),
		Encoding:         "dyn",
		EncoderConfig:    groupedEncoderConfig(),
		OutputPaths:      []string{"dyn://x"},
		ErrorOutputPaths: []string{"stderr"},
	}
	for g := 0; g < 2; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				log, err := cfg.Build()
				if err != nil {
					ok := strings.Contains(err.Error(), "no encoder registered for name \"dyn\"") ||
						strings.Contains(err.Error(), "no sink found for scheme \"dyn\"")
					if !ok {
						t.Errorf("unexpected build error: %v", err)
						return
					}
					continue
				}
				log.Info("x")
				if err := log.Sync(); err != nil {
					t.Errorf("sync: %v", err)
					return
				}
			}
		}()
	}

	wg.Wait()
}
