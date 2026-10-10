// Copyright (c) 2016 Uber Technologies, Inc.
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
	"bytes"
	"errors"
	"sync"
	"testing"

	"go.uber.org/zap/internal/exit"
	"go.uber.org/zap/internal/pool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertGoexit(t *testing.T, f func()) {
	var finished bool
	recovered := make(chan interface{})
	go func() {
		defer func() {
			recovered <- recover()
		}()

		f()
		finished = true
	}()

	assert.Nil(t, <-recovered, "Goexit should cause recover to return nil")
	assert.False(t, finished, "Goroutine should not finish after Goexit")
}

func TestPutNilEntry(t *testing.T) {
	// Pooling nil entries defeats the purpose.
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			putCheckedEntry(nil)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			ce := getCheckedEntry()
			assert.NotNil(t, ce, "Expected only non-nil CheckedEntries in pool.")
			assert.False(t, ce.dirty, "Unexpected dirty bit set.")
			assert.Nil(t, ce.ErrorOutput, "Non-nil ErrorOutput.")
			assert.Nil(t, ce.after, "Unexpected terminal behavior.")
			assert.Equal(t, 0, len(ce.cores), "Expected empty slice of cores.")
			assert.True(t, cap(ce.cores) > 0, "Expected pooled CheckedEntries to pre-allocate slice of Cores.")
			assert.Equal(t, 0, len(ce.afterWrite), "Expected empty slice of post-write hooks.")
			assert.Equal(t, 0, len(ce.branches), "Expected empty slice of branches.")
		}
	}()

	wg.Wait()
}

func TestEntryCaller(t *testing.T) {
	tests := []struct {
		caller EntryCaller
		full   string
		short  string
	}{
		{
			caller: NewEntryCaller(100, "/path/to/foo.go", 42, false),
			full:   "undefined",
			short:  "undefined",
		},
		{
			caller: NewEntryCaller(100, "/path/to/foo.go", 42, true),
			full:   "/path/to/foo.go:42",
			short:  "to/foo.go:42",
		},
		{
			caller: NewEntryCaller(100, "to/foo.go", 42, true),
			full:   "to/foo.go:42",
			short:  "to/foo.go:42",
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.full, tt.caller.String(), "Unexpected string from EntryCaller.")
		assert.Equal(t, tt.full, tt.caller.FullPath(), "Unexpected FullPath from EntryCaller.")
		assert.Equal(t, tt.short, tt.caller.TrimmedPath(), "Unexpected TrimmedPath from EntryCaller.")
	}
}

func TestCheckedEntryWrite(t *testing.T) {
	t.Run("nil is safe", func(t *testing.T) {
		var ce *CheckedEntry
		assert.NotPanics(t, func() { ce.Write() }, "Unexpected panic writing nil CheckedEntry.")
	})

	t.Run("WriteThenPanic", func(t *testing.T) {
		var ce *CheckedEntry
		ce = ce.After(Entry{}, WriteThenPanic)
		assert.Panics(t, func() { ce.Write() }, "Expected to panic when WriteThenPanic is set.")
	})

	t.Run("WriteThenGoexit", func(t *testing.T) {
		var ce *CheckedEntry
		ce = ce.After(Entry{}, WriteThenGoexit)
		assertGoexit(t, func() { ce.Write() })
	})

	t.Run("WriteThenFatal", func(t *testing.T) {
		var ce *CheckedEntry
		ce = ce.After(Entry{}, WriteThenFatal)
		stub := exit.WithStub(func() {
			ce.Write()
		})
		assert.True(t, stub.Exited, "Expected to exit when WriteThenFatal is set.")
		assert.Equal(t, 1, stub.Code, "Expected to exit when WriteThenFatal is set.")
	})

	t.Run("After", func(t *testing.T) {
		var ce *CheckedEntry
		hook := &customHook{}
		ce = ce.After(Entry{}, hook)
		ce.Write()
		assert.True(t, hook.called, "Expected to call custom action after Write.")
	})
}

type customHook struct {
	called bool
}

func (c *customHook) OnWrite(_ *CheckedEntry, _ []Field) {
	c.called = true
}

func TestCheckedEntryBefore(t *testing.T) {
	t.Run("nil is safe", func(t *testing.T) {
		var ce *CheckedEntry
		ce = ce.Before(Entry{Message: "hello"}, func(ent Entry, fields []Field) (Entry, []Field) {
			ent.Message = "modified"
			return ent, fields
		})
		assert.NotNil(t, ce)
		assert.Equal(t, "hello", ce.Message)
	})

	t.Run("modifies entry and fields", func(t *testing.T) {
		core := &recordingCore{}
		var ce *CheckedEntry
		ce = ce.AddCore(Entry{Message: "original"}, core)
		ce = ce.Before(Entry{}, func(ent Entry, fields []Field) (Entry, []Field) {
			ent.Message = "modified"
			fields = append(fields, Field{Key: "added", Type: StringType, String: "value"})
			return ent, fields
		})
		ce.Write(Field{Key: "initial", Type: StringType, String: "v"})

		assert.Equal(t, "modified", core.entry.Message)
		assert.Len(t, core.fields, 2)
		assert.Equal(t, "initial", core.fields[0].Key)
		assert.Equal(t, "added", core.fields[1].Key)
	})

	t.Run("multiple hooks chain in order", func(t *testing.T) {
		core := &recordingCore{}
		var ce *CheckedEntry
		ce = ce.AddCore(Entry{Message: "start"}, core)
		ce = ce.Before(Entry{}, func(ent Entry, fields []Field) (Entry, []Field) {
			ent.Message = ent.Message + "-first"
			return ent, fields
		})
		ce = ce.Before(Entry{}, func(ent Entry, fields []Field) (Entry, []Field) {
			ent.Message = ent.Message + "-second"
			return ent, fields
		})
		ce.Write()

		assert.Equal(t, "start-first-second", core.entry.Message)
	})

	t.Run("hooks reset on pool reuse", func(t *testing.T) {
		core := &recordingCore{}
		var ce *CheckedEntry
		ce = ce.AddCore(Entry{Message: "first"}, core)
		ce = ce.Before(Entry{}, func(ent Entry, fields []Field) (Entry, []Field) {
			ent.Message = "hooked"
			return ent, fields
		})
		ce.Write()
		assert.Equal(t, "hooked", core.entry.Message)

		// Get a new entry from the pool — hooks should be cleared.
		ce2 := getCheckedEntry()
		assert.Empty(t, ce2.before)
		putCheckedEntry(ce2)
	})
}

// recordingCore captures the last entry and fields written to it.
type recordingCore struct {
	entry  Entry
	fields []Field
	writes int
}

func (c *recordingCore) Enabled(Level) bool                              { return true }
func (c *recordingCore) With([]Field) Core                               { return c }
func (c *recordingCore) Check(ent Entry, ce *CheckedEntry) *CheckedEntry { return ce.AddCore(ent, c) }
func (c *recordingCore) Sync() error                                     { return nil }
func (c *recordingCore) Write(ent Entry, fields []Field) error {
	c.entry = ent
	c.fields = fields
	c.writes++
	return nil
}

// failingCore always selects the entry but fails to write it.
type failingCore struct {
	err error
}

func (c *failingCore) Enabled(Level) bool                              { return true }
func (c *failingCore) With([]Field) Core                               { return c }
func (c *failingCore) Check(ent Entry, ce *CheckedEntry) *CheckedEntry { return ce.AddCore(ent, c) }
func (c *failingCore) Sync() error                                     { return nil }
func (c *failingCore) Write(Entry, []Field) error                      { return c.err }

// disabledCore never selects an entry.
type disabledCore struct{}

func (disabledCore) Enabled(Level) bool                            { return false }
func (disabledCore) With([]Field) Core                             { return disabledCore{} }
func (disabledCore) Check(_ Entry, ce *CheckedEntry) *CheckedEntry { return ce }
func (disabledCore) Sync() error                                   { return nil }
func (disabledCore) Write(Entry, []Field) error                    { return nil }

// orderHook is a CheckWriteHook that records when it ran.
type orderHook struct {
	tag   string
	order *[]string
}

func (h *orderHook) OnWrite(*CheckedEntry, []Field) {
	*h.order = append(*h.order, h.tag)
}

func newCEPool() func() {
	old := _cePool
	_cePool = pool.New(func() *CheckedEntry {
		return &CheckedEntry{
			cores:    make([]Core, 4),
			branches: make([]coreBranch, 4),
		}
	})
	return func() { _cePool = old }
}

func TestCheckedEntryAfterWrite(t *testing.T) {
	ent := Entry{Message: "msg"}

	t.Run("nil is safe", func(t *testing.T) {
		var ce *CheckedEntry
		called := false
		ce = ce.AfterWrite(ent, func(Entry, []Field, []WriteResult) error {
			called = true
			return nil
		})
		require.NotNil(t, ce)
		assert.NotPanics(t, func() { ce.Write() })
		assert.True(t, called, "Expected post-write hook to run with no cores.")
	})

	t.Run("success result for implicit branch", func(t *testing.T) {
		core := &recordingCore{}
		var results []WriteResult
		var ce *CheckedEntry
		ce = ce.AddCore(ent, core)
		ce = ce.AfterWrite(ent, func(_ Entry, _ []Field, rs []WriteResult) error {
			results = rs
			return nil
		})
		ce.Write()

		require.Len(t, results, 1)
		assert.Same(t, core, results[0].Core)
		assert.Equal(t, WriteSucceeded, results[0].Status)
		assert.NoError(t, results[0].Err)
	})

	t.Run("failure result carries the real error", func(t *testing.T) {
		writeErr := errors.New("boom")
		core := &failingCore{err: writeErr}
		var results []WriteResult
		var ce *CheckedEntry
		ce = ce.AddCore(ent, core)
		ce = ce.AfterWrite(ent, func(_ Entry, _ []Field, rs []WriteResult) error {
			results = rs
			return nil
		})
		ce.Write()

		require.Len(t, results, 1)
		assert.Equal(t, WriteFailed, results[0].Status)
		assert.Equal(t, writeErr, results[0].Err)
	})

	t.Run("not selected, success, and failure in declaration order", func(t *testing.T) {
		off := disabledCore{}
		ok := &recordingCore{}
		fail := &failingCore{err: errors.New("boom")}
		okAfterFailure := &recordingCore{}
		tee := NewTee(off, ok, fail, okAfterFailure)

		var results []WriteResult
		ce := tee.Check(ent, nil)
		require.NotNil(t, ce)
		ce = ce.AfterWrite(ent, func(_ Entry, _ []Field, rs []WriteResult) error {
			results = rs
			return nil
		})
		ce.Write()

		require.Len(t, results, 4)
		assert.Equal(t, off, results[0].Core)
		assert.Equal(t, WriteNotSelected, results[0].Status)
		assert.Nil(t, results[0].Err)

		assert.Same(t, ok, results[1].Core)
		assert.Equal(t, WriteSucceeded, results[1].Status)
		assert.Equal(t, 1, ok.writes, "Successful branch must still be written.")

		assert.Same(t, fail, results[2].Core)
		assert.Equal(t, WriteFailed, results[2].Status)
		assert.Equal(t, fail.err, results[2].Err)

		assert.Same(t, okAfterFailure, results[3].Core)
		assert.Equal(t, WriteSucceeded, results[3].Status,
			"A failure must not mask branches declared later.")
		assert.Equal(t, 1, okAfterFailure.writes)
	})

	t.Run("hook errors are reported but do not stop wrap-up", func(t *testing.T) {
		var buf bytes.Buffer
		var order []string

		var ce *CheckedEntry
		ce = ce.AddCore(ent, &recordingCore{})
		ce.ErrorOutput = AddSync(&buf)
		ce = ce.AfterWrite(ent, func(Entry, []Field, []WriteResult) error {
			order = append(order, "first")
			return errors.New("hook boom")
		})
		ce = ce.AfterWrite(ent, func(Entry, []Field, []WriteResult) error {
			order = append(order, "second")
			return nil
		})
		ce = ce.After(ent, &orderHook{tag: "terminal", order: &order})
		ce.Write()

		assert.Contains(t, buf.String(), "post-write hook error: hook boom")
		assert.Equal(t, []string{"first", "second", "terminal"}, order,
			"Hook errors must not skip subsequent hooks; terminal action runs last.")
	})

	t.Run("runs before the terminal panic action", func(t *testing.T) {
		postRan := false
		var ce *CheckedEntry
		ce = ce.AddCore(ent, &recordingCore{})
		ce = ce.AfterWrite(ent, func(Entry, []Field, []WriteResult) error {
			postRan = true
			return nil
		})
		ce = ce.After(ent, WriteThenPanic)
		assert.Panics(t, func() { ce.Write() })
		assert.True(t, postRan, "Post-write processing must run before the terminal action.")
	})

	t.Run("logging again inside the hook still releases the entry", func(t *testing.T) {
		restore := newCEPool()
		defer restore()

		outer := &recordingCore{}
		nested := &recordingCore{}

		var ce *CheckedEntry
		ce = ce.AddCore(Entry{Message: "outer"}, outer)
		ce = ce.AfterWrite(ent, func(_ Entry, _ []Field, results []WriteResult) error {
			// Log again while the outer entry is checked out.
			var innerCE *CheckedEntry
			innerCE = innerCE.AddCore(Entry{Message: "nested"}, nested)
			innerCE.Write()
			// The nested cycle must not disturb the outer entry's results.
			require.Len(t, results, 1)
			assert.Equal(t, WriteSucceeded, results[0].Status)
			return nil
		})
		ce.Write()

		assert.Equal(t, "outer", outer.entry.Message)
		assert.Equal(t, "nested", nested.entry.Message)

		// Exactly two entries cycled through the fresh pool: the nested one
		// and the outer one. The outer entry must have been returned even
		// though the hook logged again.
		first := getCheckedEntry()
		second := getCheckedEntry()
		assert.True(t, first == ce || second == ce,
			"Expected the outer CheckedEntry to be returned to the pool.")
		putCheckedEntry(first)
		putCheckedEntry(second)
	})

	t.Run("entry is released when a post-write hook panics", func(t *testing.T) {
		restore := newCEPool()
		defer restore()

		var ce *CheckedEntry
		ce = ce.AddCore(ent, &recordingCore{})
		ce = ce.AfterWrite(ent, func(Entry, []Field, []WriteResult) error {
			panic("hook panic")
		})
		assert.Panics(t, func() { ce.Write() })

		reused := getCheckedEntry()
		assert.Same(t, ce, reused, "Expected the panicked entry to be returned to the pool.")
		assert.False(t, reused.dirty)
		assert.Empty(t, reused.cores)
		putCheckedEntry(reused)
	})

	t.Run("state resets on pool reuse", func(t *testing.T) {
		core := &recordingCore{}
		var ce *CheckedEntry
		ce = ce.AddCore(Entry{Message: "first"}, core)
		ce = ce.AfterWrite(ent, func(Entry, []Field, []WriteResult) error { return nil })
		ce.Write()

		reused := getCheckedEntry()
		assert.Empty(t, reused.afterWrite)
		assert.Empty(t, reused.branches)
		putCheckedEntry(reused)
	})
}
