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

package zapcore_test

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func fieldLimitEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey: "msg",
	}
}

func requireFieldLimitError(t *testing.T, err error, reason zapcore.FieldLimitReason) *zapcore.FieldLimitError {
	t.Helper()
	var limitErr *zapcore.FieldLimitError
	require.ErrorAs(t, err, &limitErr)
	assert.ErrorIs(t, err, zapcore.ErrFieldLimitExceeded)
	assert.Equal(t, reason, limitErr.Reason)
	return limitErr
}

func TestFieldLimitCoreDropsAndRecovers(t *testing.T) {
	var (
		sink     bytes.Buffer
		reports  []zapcore.FieldLimitError
		reportMu sync.Mutex
	)
	cfg := fieldLimitEncoderConfig()
	cfg.MaxFields = 2
	cfg.OnFieldLimit = func(_ zapcore.Entry, e zapcore.FieldLimitError) {
		reportMu.Lock()
		reports = append(reports, e)
		reportMu.Unlock()
	}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg),
		zapcore.AddSync(&sink),
		zapcore.DebugLevel,
	)
	ent := zapcore.Entry{Level: zapcore.InfoLevel}

	// The third business field trips the limit: the whole entry is dropped.
	err := core.Write(ent, []zap.Field{
		zap.String("a", "1"),
		zap.String("b", "2"),
		zap.String("c", "3"),
	})
	limitErr := requireFieldLimitError(t, err, zapcore.FieldLimitFieldsExceeded)
	assert.Equal(t, 3, limitErr.Fields)
	assert.Equal(t, "c", limitErr.Key)
	assert.Empty(t, sink.String())

	// Entries within the limit encode normally; one rejection doesn't poison
	// the logger.
	require.NoError(t, core.Write(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: "fine"},
		[]zap.Field{zap.String("a", "1")},
	))
	assert.Contains(t, sink.String(), `"msg":"fine"`)
	assert.Contains(t, sink.String(), `"a":"1"`)

	require.Len(t, reports, 1)
}

func TestFieldLimitInheritedFields(t *testing.T) {
	t.Run("inherited plus entry fields within limit", func(t *testing.T) {
		var sink bytes.Buffer
		cfg := fieldLimitEncoderConfig()
		cfg.MaxFields = 2
		core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), zapcore.AddSync(&sink), zapcore.DebugLevel)
		child := core.With([]zap.Field{zap.String("inherited", "x")})

		require.NoError(t, child.Write(
			zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"},
			[]zap.Field{zap.String("site", "y")},
		))
		assert.Contains(t, sink.String(), `"inherited":"x"`)
		assert.Contains(t, sink.String(), `"site":"y"`)
	})

	t.Run("inherited fields count toward the limit", func(t *testing.T) {
		var sink bytes.Buffer
		cfg := fieldLimitEncoderConfig()
		cfg.MaxFields = 2
		core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), zapcore.AddSync(&sink), zapcore.DebugLevel)
		child := core.With([]zap.Field{zap.String("a", "1"), zap.String("b", "2")})

		err := child.Write(
			zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"},
			[]zap.Field{zap.String("c", "3")},
		)
		requireFieldLimitError(t, err, zapcore.FieldLimitFieldsExceeded)
		assert.Empty(t, sink.String())
	})

	t.Run("over-budget context rejects every entry, once each", func(t *testing.T) {
		var (
			sink    bytes.Buffer
			calls   int
			callsMu sync.Mutex
		)
		cfg := fieldLimitEncoderConfig()
		cfg.MaxFields = 2
		cfg.OnFieldLimit = func(_ zapcore.Entry, _ zapcore.FieldLimitError) {
			callsMu.Lock()
			calls++
			callsMu.Unlock()
		}
		core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), zapcore.AddSync(&sink), zapcore.DebugLevel)
		// Three inherited fields already exceed the limit.
		child := core.With([]zap.Field{
			zap.String("a", "1"),
			zap.String("b", "2"),
			zap.String("c", "3"),
		})

		require.Error(t, child.Write(zapcore.Entry{Level: zapcore.InfoLevel, Message: "one"}, nil))
		require.Error(t, child.Write(zapcore.Entry{Level: zapcore.InfoLevel, Message: "two"}, nil))
		assert.Equal(t, 2, calls)
		assert.Empty(t, sink.String())

		// The parent logger is unaffected and keeps logging normally.
		require.NoError(t, core.Write(zapcore.Entry{Level: zapcore.InfoLevel, Message: "parent"}, nil))
		assert.Contains(t, sink.String(), `"msg":"parent"`)
	})
}

// countingObject records how often it is marshaled and emits the configured
// number of integer fields.
type countingObject struct {
	calls       int
	innerFields int
}

func (c *countingObject) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	c.calls++
	for i := 0; i < c.innerFields; i++ {
		enc.AddInt("f", i)
	}
	return nil
}

type countingStringer struct{ calls int }

func (s *countingStringer) String() string {
	s.calls++
	return "s"
}

func TestFieldLimitStopsExpandingSubsequentFields(t *testing.T) {
	cfg := fieldLimitEncoderConfig()
	cfg.MaxFields = 2
	core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), nil, zapcore.DebugLevel)

	notExpanded := &countingObject{innerFields: 1}
	notStringified := &countingStringer{}

	err := core.Write(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"},
		[]zap.Field{
			zap.String("a", "1"),
			zap.String("b", "2"),
			zap.Object("c", notExpanded),      // would be field 3
			zap.Stringer("d", notStringified), // would be field 4
		},
	)
	requireFieldLimitError(t, err, zapcore.FieldLimitFieldsExceeded)
	assert.Equal(t, 0, notExpanded.calls, "object past the limit must not be marshaled")
	assert.Equal(t, 0, notStringified.calls, "stringer past the limit must not be evaluated")
}

func TestFieldLimitStopsWithinObject(t *testing.T) {
	cfg := fieldLimitEncoderConfig()
	cfg.MaxFields = 3
	core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), nil, zapcore.DebugLevel)

	// The object key (1) plus two inner fields (2, 3) fit; the third inner
	// field trips the limit and the entry is dropped altogether.
	obj := &countingObject{innerFields: 5}
	err := core.Write(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"},
		[]zap.Field{zap.Object("o", obj)},
	)
	limitErr := requireFieldLimitError(t, err, zapcore.FieldLimitFieldsExceeded)
	assert.Equal(t, 1, obj.calls)
	assert.Equal(t, 4, limitErr.Fields)
	assert.Equal(t, "f", limitErr.Key)
}

func TestFieldLimitDepthObjectAndNamespace(t *testing.T) {
	cfg := fieldLimitEncoderConfig()
	cfg.MaxDepth = 1
	core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), nil, zapcore.DebugLevel)

	// Namespace opens depth 1; an object inside it reaches depth 2 and must
	// not be expanded.
	notExpanded := &countingObject{innerFields: 1}
	err := core.Write(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"},
		[]zap.Field{
			zap.Namespace("ns"),
			zap.Object("o", notExpanded),
		},
	)
	requireFieldLimitError(t, err, zapcore.FieldLimitDepthExceeded)
	assert.Equal(t, 0, notExpanded.calls)

	// A primitive field inside the namespace fits within depth 1.
	var sink bytes.Buffer
	okCore := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), zapcore.AddSync(&sink), zapcore.DebugLevel)
	require.NoError(t, okCore.Write(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"},
		[]zap.Field{zap.Namespace("ns"), zap.String("a", "1")},
	))
	assert.Contains(t, sink.String(), `"ns":{"a":"1"`)
}

// verboseError implements fmt.Formatter, so encoding it emits both the
// "error" and "errorVerbose" fields.
type verboseError struct{}

func (verboseError) Error() string          { return "boom" }
func (verboseError) Format(fmt.State, rune) {}

func TestFieldLimitCountsErrorFields(t *testing.T) {
	cfg := fieldLimitEncoderConfig()
	cfg.MaxFields = 1
	core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), nil, zapcore.DebugLevel)

	err := core.Write(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"},
		[]zap.Field{zap.NamedError("err", verboseError{})},
	)
	limitErr := requireFieldLimitError(t, err, zapcore.FieldLimitFieldsExceeded)
	// "err" plus "errVerbose".
	assert.Equal(t, 2, limitErr.Fields)
	assert.Equal(t, "errVerbose", limitErr.Key)
}

func TestFieldLimitConsoleEncoder(t *testing.T) {
	var (
		sink    bytes.Buffer
		calls   int
		callsMu sync.Mutex
	)
	cfg := fieldLimitEncoderConfig()
	cfg.MaxFields = 1
	cfg.OnFieldLimit = func(ent zapcore.Entry, e zapcore.FieldLimitError) {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		assert.Equal(t, "m", ent.Message)
		assert.Equal(t, zapcore.FieldLimitFieldsExceeded, e.Reason)
	}
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(cfg), zapcore.AddSync(&sink), zapcore.DebugLevel)

	require.Error(t, core.Write(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: "m"},
		[]zap.Field{zap.String("a", "1"), zap.String("b", "2")},
	))
	assert.Empty(t, sink.String())

	require.NoError(t, core.Write(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: "fine"},
		[]zap.Field{zap.String("a", "1")},
	))
	assert.Contains(t, sink.String(), "fine")
	assert.Equal(t, 1, calls)
}

func TestFieldLimitLoggerEndToEnd(t *testing.T) {
	var sink, errSink bytes.Buffer
	cfg := fieldLimitEncoderConfig()
	cfg.MaxFields = 1
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg),
		zapcore.AddSync(&sink),
		zapcore.DebugLevel,
	)
	log := zap.New(core, zap.ErrorOutput(zapcore.AddSync(&errSink)))

	log.Info("too many", zap.String("a", "1"), zap.String("b", "2"))
	log.Info("just right", zap.String("a", "1"))

	assert.NotContains(t, sink.String(), "too many")
	assert.Contains(t, sink.String(), "just right")
	// The rejection is reported on the logger's error output as a write error.
	assert.Contains(t, errSink.String(), "write error")
	assert.Contains(t, errSink.String(), "field limit exceeded")
}

func TestFieldLimitSugaredLogger(t *testing.T) {
	var sink bytes.Buffer
	cfg := fieldLimitEncoderConfig()
	cfg.MaxFields = 1
	core := zapcore.NewCore(zapcore.NewJSONEncoder(cfg), zapcore.AddSync(&sink), zapcore.DebugLevel)
	sugar := zap.New(core).Sugar()

	sugar.Infow("sugar bad", "a", 1, "b", 2)
	sugar.Infow("sugar good", "a", 1)

	assert.NotContains(t, sink.String(), "sugar bad")
	assert.Contains(t, sink.String(), "sugar good")
}

func TestFieldLimitConcurrentEntries(t *testing.T) {
	var (
		sink       bytes.Buffer
		rejections int
		mu         sync.Mutex
	)
	cfg := fieldLimitEncoderConfig()
	cfg.MaxFields = 3
	cfg.OnFieldLimit = func(_ zapcore.Entry, _ zapcore.FieldLimitError) {
		mu.Lock()
		rejections++
		mu.Unlock()
	}
	// The sink is shared across goroutines, so it must be concurrency-safe.
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(cfg),
		zapcore.Lock(zapcore.AddSync(&sink)),
		zapcore.DebugLevel,
	)

	overFields := []zap.Field{
		zap.String("a", "1"),
		zap.String("b", "2"),
		zap.String("c", "3"),
		zap.String("d", "4"),
		zap.String("e", "5"),
	}
	underFields := []zap.Field{
		zap.String("a", "1"),
		zap.String("b", "2"),
	}

	const goroutines = 16
	const iterations = 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		over := g%2 == 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				fs := underFields
				msg := "under"
				if over {
					fs = overFields
					msg = "over"
				}
				_ = core.Write(zapcore.Entry{Level: zapcore.InfoLevel, Message: msg}, fs)
			}
		}()
	}
	wg.Wait()

	// Exactly the over-limit half of the writes are reported.
	assert.Equal(t, goroutines/2*iterations, rejections)

	// The logger still works and each entry is counted independently.
	require.NoError(t, core.Write(
		zapcore.Entry{Level: zapcore.InfoLevel, Message: "recovery"},
		[]zap.Field{zap.String("a", "1")},
	))
	assert.Contains(t, sink.String(), `"msg":"recovery"`)
}
