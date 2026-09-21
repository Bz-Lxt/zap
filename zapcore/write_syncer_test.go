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
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/internal/ztest"
)

type writeSyncSpy struct {
	io.Writer
	ztest.Syncer
}

func requireWriteWorks(t testing.TB, ws WriteSyncer) {
	n, err := ws.Write([]byte("foo"))
	require.NoError(t, err, "Unexpected error writing to WriteSyncer.")
	require.Equal(t, 3, n, "Wrote an unexpected number of bytes.")
}

func TestAddSyncWriteSyncer(t *testing.T) {
	buf := &bytes.Buffer{}
	concrete := &writeSyncSpy{Writer: buf}
	ws := AddSync(concrete)
	requireWriteWorks(t, ws)

	require.NoError(t, ws.Sync(), "Unexpected error syncing a WriteSyncer.")
	require.True(t, concrete.Called(), "Expected to dispatch to concrete type's Sync method.")

	concrete.SetError(errors.New("fail"))
	assert.Error(t, ws.Sync(), "Expected to propagate errors from concrete type's Sync method.")
}

func TestAddSyncWriter(t *testing.T) {
	// If we pass a plain io.Writer, make sure that we still get a WriteSyncer
	// with a no-op Sync.
	buf := &bytes.Buffer{}
	ws := AddSync(buf)
	requireWriteWorks(t, ws)
	assert.NoError(t, ws.Sync(), "Unexpected error calling a no-op Sync method.")
}

func TestNewMultiWriteSyncerWorksForSingleWriter(t *testing.T) {
	w := &ztest.Buffer{}

	ws := NewMultiWriteSyncer(w)
	assert.Equal(t, w, ws, "Expected NewMultiWriteSyncer to return the same WriteSyncer object for a single argument.")

	assert.NoError(t, ws.Sync(), "Expected Sync to succeed.")
	assert.True(t, w.Called(), "Expected Sync to be called on the created WriteSyncer")
}

func TestMultiWriteSyncerWritesBoth(t *testing.T) {
	first := &bytes.Buffer{}
	second := &bytes.Buffer{}
	ws := NewMultiWriteSyncer(AddSync(first), AddSync(second))

	msg := []byte("dumbledore")
	n, err := ws.Write(msg)
	require.NoError(t, err, "Expected successful buffer write")
	assert.Equal(t, len(msg), n)

	assert.Equal(t, msg, first.Bytes())
	assert.Equal(t, msg, second.Bytes())
}

func TestMultiWriteSyncerFailsWrite(t *testing.T) {
	ws := NewMultiWriteSyncer(AddSync(&ztest.FailWriter{}))
	_, err := ws.Write([]byte("test"))
	assert.Error(t, err, "Write error should propagate")
}

func TestMultiWriteSyncerFailsShortWrite(t *testing.T) {
	ws := NewMultiWriteSyncer(AddSync(&ztest.ShortWriter{}))
	n, err := ws.Write([]byte("test"))
	assert.NoError(t, err, "Expected fake-success from short write")
	assert.Equal(t, 3, n, "Expected byte count to return from underlying writer")
}

func TestWritestoAllSyncs_EvenIfFirstErrors(t *testing.T) {
	failer := &ztest.FailWriter{}
	second := &bytes.Buffer{}
	ws := NewMultiWriteSyncer(AddSync(failer), AddSync(second))

	_, err := ws.Write([]byte("fail"))
	assert.Error(t, err, "Expected error from call to a writer that failed")
	assert.Equal(t, []byte("fail"), second.Bytes(), "Expected second sink to be written after first error")
}

func TestMultiWriteSyncerSync_PropagatesErrors(t *testing.T) {
	badsink := &ztest.Buffer{}
	badsink.SetError(errors.New("sink is full"))
	ws := NewMultiWriteSyncer(&ztest.Discarder{}, badsink)

	assert.Error(t, ws.Sync(), "Expected sync error to propagate")
}

func TestMultiWriteSyncerSync_NoErrorsOnDiscard(t *testing.T) {
	ws := NewMultiWriteSyncer(&ztest.Discarder{})
	assert.NoError(t, ws.Sync(), "Expected error-free sync to /dev/null")
}

func TestMultiWriteSyncerSync_AllCalled(t *testing.T) {
	failed, second := &ztest.Buffer{}, &ztest.Buffer{}

	failed.SetError(errors.New("disposal broken"))
	ws := NewMultiWriteSyncer(failed, second)

	assert.Error(t, ws.Sync(), "Expected first sink to fail")
	assert.True(t, failed.Called(), "Expected first sink to have Sync method called.")
	assert.True(t, second.Called(), "Expected call to Sync even with first failure.")
}

func drainAs(t *testing.T, ws WriteSyncer) DrainingWriteSyncer {
	t.Helper()
	d, ok := ws.(DrainingWriteSyncer)
	require.True(t, ok, "expected %T to support Drain", ws)
	return d
}

func waitForSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(ztest.Timeout(5 * time.Second)):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestMultiWriteSyncerDrain(t *testing.T) {
	t.Run("drains all syncers", func(t *testing.T) {
		first, second := &ztest.Buffer{}, &ztest.Buffer{}
		ws := drainAs(t, NewMultiWriteSyncer(first, second))

		assert.NoError(t, ws.Drain(context.Background()))
		assert.True(t, first.Called(), "expected first Sync to run")
		assert.True(t, second.Called(), "expected second Sync to run")
	})

	t.Run("combines errors from all syncers", func(t *testing.T) {
		first := &ztest.Buffer{}
		first.SetError(errors.New("first failed"))
		second := &ztest.Buffer{}
		second.SetError(errors.New("second failed"))
		ws := drainAs(t, NewMultiWriteSyncer(first, second))

		err := ws.Drain(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "first failed")
		assert.Contains(t, err.Error(), "second failed")
	})

	t.Run("drains outputs in parallel and resumes after timeout", func(t *testing.T) {
		first := newGateWriteSyncer(false, true)
		second := newGateWriteSyncer(false, true)
		t.Cleanup(func() {
			first.releaseSyncs()
			second.releaseSyncs()
		})
		ws := drainAs(t, NewMultiWriteSyncer(first, second))

		ctx, cancel := context.WithTimeout(
			context.Background(), ztest.Timeout(30*time.Millisecond))
		defer cancel()
		require.ErrorIs(t, ws.Drain(ctx), context.DeadlineExceeded)

		// With a sequential implementation the second Sync would never be
		// reached while the first is blocked.
		waitForSignal(t, first.syncEnter, "first sync")
		waitForSignal(t, second.syncEnter, "second sync")

		// The timed-out drain completes in the background once released; a
		// follow-up drain succeeds.
		first.releaseSyncs()
		second.releaseSyncs()
		require.NoError(t, ws.Drain(context.Background()))

		first.mu.Lock()
		assert.GreaterOrEqual(t, first.syncs, 1)
		first.mu.Unlock()
		second.mu.Lock()
		assert.GreaterOrEqual(t, second.syncs, 1)
		second.mu.Unlock()
	})

	t.Run("canceled context skips all syncers", func(t *testing.T) {
		first, second := &ztest.Buffer{}, &ztest.Buffer{}
		ws := drainAs(t, NewMultiWriteSyncer(first, second))

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, ws.Drain(ctx), context.Canceled)
		assert.False(t, first.Called(), "no sync should run on a pre-canceled drain")
		assert.False(t, second.Called())
	})
}

func TestLockedWriteSyncerDrain(t *testing.T) {
	t.Run("drains unlocked syncer", func(t *testing.T) {
		ws := drainAs(t, Lock(&ztest.Discarder{}))
		assert.NoError(t, ws.Drain(context.Background()))
	})

	t.Run("honors deadline and resumes", func(t *testing.T) {
		g := newGateWriteSyncer(false, true)
		t.Cleanup(g.releaseSyncs)
		ws := drainAs(t, Lock(g))

		ctx, cancel := context.WithTimeout(
			context.Background(), ztest.Timeout(30*time.Millisecond))
		defer cancel()
		require.ErrorIs(t, ws.Drain(ctx), context.DeadlineExceeded)

		g.releaseSyncs()
		assert.NoError(t, ws.Drain(context.Background()))
	})
}
