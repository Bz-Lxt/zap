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

package zapcore_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	//revive:disable:dot-imports
	. "go.uber.org/zap/zapcore"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingWriteSyncer records every Write call with its own copy of the
// payload.
type recordingWriteSyncer struct {
	writes [][]byte
}

func (w *recordingWriteSyncer) Write(p []byte) (int, error) {
	w.writes = append(w.writes, append([]byte(nil), p...))
	return len(p), nil
}

func (*recordingWriteSyncer) Sync() error { return nil }

// failingWriteSyncer rejects every write without accepting any bytes.
type failingWriteSyncer struct {
	err error
}

func (w *failingWriteSyncer) Write([]byte) (int, error) { return 0, w.err }
func (w *failingWriteSyncer) Sync() error               { return nil }

// partialWriteSyncer accepts only the first acknowledged bytes of the first
// write with an error, then accepts every following write fully.
type partialWriteSyncer struct {
	calls       [][]byte
	acknowledge int
}

func (w *partialWriteSyncer) Write(p []byte) (int, error) {
	w.calls = append(w.calls, append([]byte(nil), p...))
	if len(w.calls) == 1 {
		n := w.acknowledge
		if n > len(p) {
			n = len(p)
		}
		return n, errors.New("partial write")
	}
	return len(p), nil
}

func (*partialWriteSyncer) Sync() error { return nil }

// plainCore is a minimal Core that intentionally does NOT implement
// BatchCore, so the driver must fall back to per-entry Write calls.
type plainCore struct {
	entries []Entry
}

func (*plainCore) Enabled(Level) bool { return true }
func (*plainCore) With([]Field) Core  { return &plainCore{} }

func (c *plainCore) Check(ent Entry, ce *CheckedEntry) *CheckedEntry {
	return ce.AddCore(ent, c)
}

func (c *plainCore) Write(ent Entry, _ []Field) error {
	c.entries = append(c.entries, ent)
	return nil
}

func (*plainCore) Sync() error { return nil }

func batchEncoderConfig() EncoderConfig {
	cfg := testEncoderConfig()
	cfg.TimeKey = "" // timestamps complicate payload assertions
	return cfg
}

func batchEntries() []BatchEntry {
	return []BatchEntry{
		{Entry: Entry{Level: InfoLevel, Message: "one"}},
		{Entry: Entry{Level: InfoLevel, Message: "two"}},
		{Entry: Entry{Level: InfoLevel, Message: "three"}},
	}
}

func TestWriteBatch_Empty(t *testing.T) {
	ws := &recordingWriteSyncer{}
	core := NewCore(NewJSONEncoder(batchEncoderConfig()), ws, InfoLevel)

	assert.NoError(t, WriteBatch(core, nil))
	assert.Empty(t, ws.writes)
}

func TestWriteBatch_SingleWriteInOrder(t *testing.T) {
	ws := &recordingWriteSyncer{}
	core := NewCore(NewJSONEncoder(batchEncoderConfig()), ws, DebugLevel)

	require.NoError(t, WriteBatch(core, batchEntries()))

	require.Len(t, ws.writes, 1, "Expected a single write for the whole batch")
	lines := strings.Split(strings.TrimRight(string(ws.writes[0]), "\n"), "\n")
	require.Len(t, lines, 3)
	assert.Contains(t, lines[0], `"msg":"one"`)
	assert.Contains(t, lines[1], `"msg":"two"`)
	assert.Contains(t, lines[2], `"msg":"three"`)
}

func TestWriteBatch_LevelFiltering(t *testing.T) {
	ws := &recordingWriteSyncer{}
	core := NewCore(NewJSONEncoder(batchEncoderConfig()), ws, InfoLevel)

	entries := []BatchEntry{
		{Entry: Entry{Level: DebugLevel, Message: "debug"}},
		{Entry: Entry{Level: InfoLevel, Message: "info"}},
		{Entry: Entry{Level: WarnLevel, Message: "warn"}},
		{Entry: Entry{Level: ErrorLevel, Message: "error"}},
	}
	require.NoError(t, WriteBatch(core, entries))

	require.Len(t, ws.writes, 1)
	assert.NotContains(t, string(ws.writes[0]), `"debug"`)
	assert.Contains(t, string(ws.writes[0]), `"msg":"info"`)
	assert.Contains(t, string(ws.writes[0]), `"msg":"warn"`)
	assert.Contains(t, string(ws.writes[0]), `"msg":"error"`)
}

func TestWriteBatch_Sampler(t *testing.T) {
	ws := &recordingWriteSyncer{}
	downstream := NewCore(NewJSONEncoder(batchEncoderConfig()), ws, DebugLevel)
	// Log first 2, then every 3rd; six identical entries admit #1, #2, #5.
	core := NewSamplerWithOptions(downstream, time.Second, 2, 3)

	entries := make([]BatchEntry, 6)
	for i := range entries {
		entries[i] = BatchEntry{Entry: Entry{Level: InfoLevel, Message: "repeat"}}
	}
	require.NoError(t, WriteBatch(core, entries))

	require.Len(t, ws.writes, 1)
	lines := strings.Split(strings.TrimRight(string(ws.writes[0]), "\n"), "\n")
	assert.Len(t, lines, 3)
}

func TestWriteBatch_PartialWriteRetriesTail(t *testing.T) {
	ws := &partialWriteSyncer{acknowledge: 3}
	core := NewCore(NewJSONEncoder(batchEncoderConfig()), ws, DebugLevel)

	entries := batchEntries()
	require.NoError(t, WriteBatch(core, entries))

	// Reconstruct the expected full payload by encoding the same batch into
	// a reference recording syncer.
	expected := &recordingWriteSyncer{}
	reference := NewCore(NewJSONEncoder(batchEncoderConfig()), expected, DebugLevel)
	require.NoError(t, WriteBatch(reference, entries))
	want := expected.writes[0]

	require.Len(t, ws.calls, 2, "Expected initial partial write plus one retry")
	assert.Equal(t, want, ws.calls[0])
	assert.Equal(t, want[3:], ws.calls[1], "Retry must contain only the unacknowledged tail")
}

func TestWriteBatch_FailedOutputDoesNotBlockOthers(t *testing.T) {
	writeErr := errors.New("output unavailable")
	bad := &failingWriteSyncer{err: writeErr}
	good := &recordingWriteSyncer{}

	core := NewTee(
		NewCore(NewJSONEncoder(batchEncoderConfig()), bad, DebugLevel),
		NewCore(NewJSONEncoder(batchEncoderConfig()), good, DebugLevel),
	)

	err := WriteBatch(core, batchEntries())
	assert.ErrorIs(t, err, writeErr)

	require.Len(t, good.writes, 1, "Healthy output must still complete")
	lines := strings.Split(strings.TrimRight(string(good.writes[0]), "\n"), "\n")
	assert.Len(t, lines, 3)
}

func TestWriteBatch_NonBatchCoreFallback(t *testing.T) {
	core := &plainCore{}

	require.NoError(t, WriteBatch(core, batchEntries()))

	require.Len(t, core.entries, 3)
	assert.Equal(t, "one", core.entries[0].Message)
	assert.Equal(t, "two", core.entries[1].Message)
	assert.Equal(t, "three", core.entries[2].Message)
}
