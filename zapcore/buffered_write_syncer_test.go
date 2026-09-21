// Copyright (c) 2021 Uber Technologies, Inc.
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/internal/ztest"
)

func TestBufferWriter(t *testing.T) {
	// If we pass a plain io.Writer, make sure that we still get a WriteSyncer
	// with a no-op Sync.
	t.Run("sync", func(t *testing.T) {
		buf := &bytes.Buffer{}
		ws := &BufferedWriteSyncer{WS: AddSync(buf)}

		requireWriteWorks(t, ws)
		assert.Empty(t, buf.String(), "Unexpected log calling a no-op Write method.")
		assert.NoError(t, ws.Sync(), "Unexpected error calling a no-op Sync method.")
		assert.Equal(t, "foo", buf.String(), "Unexpected log string")
		assert.NoError(t, ws.Stop())
	})

	t.Run("stop", func(t *testing.T) {
		buf := &bytes.Buffer{}
		ws := &BufferedWriteSyncer{WS: AddSync(buf)}
		requireWriteWorks(t, ws)
		assert.Empty(t, buf.String(), "Unexpected log calling a no-op Write method.")
		assert.NoError(t, ws.Stop())
		assert.Equal(t, "foo", buf.String(), "Unexpected log string")
	})

	t.Run("stop race with flush", func(t *testing.T) {
		buf := &bytes.Buffer{}
		ws := &BufferedWriteSyncer{WS: AddSync(buf), FlushInterval: 1}
		requireWriteWorks(t, ws)
		assert.NoError(t, ws.Stop())
		assert.Equal(t, "foo", buf.String(), "Unexpected log string")
	})

	t.Run("stop twice", func(t *testing.T) {
		ws := &BufferedWriteSyncer{WS: &ztest.FailWriter{}}
		_, err := ws.Write([]byte("foo"))
		require.NoError(t, err, "Unexpected error writing to WriteSyncer.")
		assert.Error(t, ws.Stop(), "Expected stop to fail.")
		assert.NoError(t, ws.Stop(), "Expected stop to not fail.")
	})

	t.Run("wrap twice", func(t *testing.T) {
		buf := &bytes.Buffer{}
		bufsync := &BufferedWriteSyncer{WS: AddSync(buf)}
		ws := &BufferedWriteSyncer{WS: bufsync}
		requireWriteWorks(t, ws)
		assert.Empty(t, buf.String(), "Unexpected log calling a no-op Write method.")
		require.NoError(t, ws.Sync())
		assert.Equal(t, "foo", buf.String())
		assert.NoError(t, ws.Stop())
		assert.NoError(t, bufsync.Stop())
		assert.Equal(t, "foo", buf.String(), "Unexpected log string")
	})

	t.Run("small buffer", func(t *testing.T) {
		buf := &bytes.Buffer{}
		ws := &BufferedWriteSyncer{WS: AddSync(buf), Size: 5}

		requireWriteWorks(t, ws)
		assert.Equal(t, "", buf.String(), "Unexpected log calling a no-op Write method.")
		requireWriteWorks(t, ws)
		assert.Equal(t, "foo", buf.String(), "Unexpected log string")
		assert.NoError(t, ws.Stop())
	})

	t.Run("with lockedWriteSyncer", func(t *testing.T) {
		buf := &bytes.Buffer{}
		ws := &BufferedWriteSyncer{WS: Lock(AddSync(buf)), Size: 5}

		requireWriteWorks(t, ws)
		assert.Equal(t, "", buf.String(), "Unexpected log calling a no-op Write method.")
		requireWriteWorks(t, ws)
		assert.Equal(t, "foo", buf.String(), "Unexpected log string")
		assert.NoError(t, ws.Stop())
	})

	t.Run("flush error", func(t *testing.T) {
		ws := &BufferedWriteSyncer{WS: &ztest.FailWriter{}, Size: 4}
		n, err := ws.Write([]byte("foo"))
		require.NoError(t, err, "Unexpected error writing to WriteSyncer.")
		require.Equal(t, 3, n, "Wrote an unexpected number of bytes.")
		_, err = ws.Write([]byte("foo"))
		assert.Error(t, err, "Expected error writing to WriteSyncer.")
		assert.Error(t, ws.Stop(), "Expected stop to fail.")
	})

	t.Run("flush timer", func(t *testing.T) {
		buf := &bytes.Buffer{}
		clock := ztest.NewMockClock()
		ws := &BufferedWriteSyncer{
			WS:            AddSync(buf),
			Size:          6,
			FlushInterval: time.Microsecond,
			Clock:         clock,
		}
		requireWriteWorks(t, ws)
		clock.Add(10 * time.Microsecond)
		assert.Equal(t, "foo", buf.String(), "Unexpected log string")

		// flush twice to validate loop logic
		requireWriteWorks(t, ws)
		clock.Add(10 * time.Microsecond)
		assert.Equal(t, "foofoo", buf.String(), "Unexpected log string")
		assert.NoError(t, ws.Stop())
	})
}

func TestBufferWriterWithoutStart(t *testing.T) {
	t.Run("stop", func(t *testing.T) {
		ws := &BufferedWriteSyncer{WS: AddSync(new(bytes.Buffer))}
		assert.NoError(t, ws.Stop(), "Stop must not fail")
	})

	t.Run("Sync", func(t *testing.T) {
		ws := &BufferedWriteSyncer{WS: AddSync(new(bytes.Buffer))}
		assert.NoError(t, ws.Sync(), "Sync must not fail")
	})
}

// gateWriteSyncer is a WriteSyncer whose Write and/or Sync calls block until
// the corresponding gate is released. The "entered" channels receive one
// signal the first time a (potentially blocked) call arrives.
type gateWriteSyncer struct {
	writeGate chan struct{} // nil means writes do not block
	syncGate  chan struct{} // nil means syncs do not block

	releaseWritesOnce sync.Once
	releaseSyncsOnce  sync.Once
	entered           chan struct{}
	syncEnter         chan struct{}

	mu    sync.Mutex
	buf   bytes.Buffer
	syncs int
}

func newGateWriteSyncer(blockWrites, blockSyncs bool) *gateWriteSyncer {
	g := &gateWriteSyncer{
		entered:   make(chan struct{}, 1),
		syncEnter: make(chan struct{}, 1),
	}
	if blockWrites {
		g.writeGate = make(chan struct{})
	}
	if blockSyncs {
		g.syncGate = make(chan struct{})
	}
	return g
}

func (g *gateWriteSyncer) releaseWrites() {
	if g.writeGate != nil {
		g.releaseWritesOnce.Do(func() { close(g.writeGate) })
	}
}

func (g *gateWriteSyncer) releaseSyncs() {
	if g.syncGate != nil {
		g.releaseSyncsOnce.Do(func() { close(g.syncGate) })
	}
}

func (g *gateWriteSyncer) Write(bs []byte) (int, error) {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	if g.writeGate != nil {
		<-g.writeGate
	}
	g.mu.Lock()
	g.buf.Write(bs)
	g.mu.Unlock()
	return len(bs), nil
}

func (g *gateWriteSyncer) Sync() error {
	select {
	case g.syncEnter <- struct{}{}:
	default:
	}
	if g.syncGate != nil {
		<-g.syncGate
	}
	g.mu.Lock()
	g.syncs++
	g.mu.Unlock()
	return nil
}

func (g *gateWriteSyncer) String() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.buf.String()
}

func drainTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), ztest.Timeout(d))
}

func drainSoon(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := drainTimeout(30 * time.Millisecond)
	t.Cleanup(cancel)
	return ctx, cancel
}

func drainLong(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := drainTimeout(5 * time.Second)
	t.Cleanup(cancel)
	return ctx, cancel
}

func TestBufferedWriteSyncerDrain(t *testing.T) {
	t.Run("flushes accepted data", func(t *testing.T) {
		buf := &bytes.Buffer{}
		ws := &BufferedWriteSyncer{WS: AddSync(buf)}

		requireWriteWorks(t, ws)
		require.NoError(t, ws.Drain(context.Background()))
		assert.Equal(t, "foo", buf.String())
		assert.NoError(t, ws.Stop())
	})

	t.Run("nil context is allowed", func(t *testing.T) {
		buf := &bytes.Buffer{}
		ws := &BufferedWriteSyncer{WS: AddSync(buf)}
		requireWriteWorks(t, ws)
		var ctx context.Context // nil ctx is treated as context.Background
		require.NoError(t, ws.Drain(ctx))
		assert.Equal(t, "foo", buf.String())
		assert.NoError(t, ws.Stop())
	})

	t.Run("times out and can be resumed without rewriting", func(t *testing.T) {
		// Size 3 makes each 3-byte write fill one segment.
		g := newGateWriteSyncer(true, false)
		ws := &BufferedWriteSyncer{WS: g, Size: 3}
		defer func() {
			g.releaseWrites()
			assert.NoError(t, ws.Stop())
		}()

		_, err := ws.Write([]byte("AAA"))
		require.NoError(t, err)

		// First drain hits the hard deadline while the first segment is
		// blocked in the underlying writer.
		ctx, cancel := drainSoon(t)
		done := make(chan error, 1)
		go func() { done <- ws.Drain(ctx) }()
		select {
		case err := <-done:
			cancel()
			require.ErrorIs(t, err, context.DeadlineExceeded)
		case <-time.After(ztest.Timeout(5 * time.Second)):
			t.Fatal("Drain did not return when the deadline expired")
		}

		// Unblock the stranded write; it completes in the background.
		g.releaseWrites()
		require.Eventually(t, func() bool {
			return g.String() == "AAA"
		}, ztest.Timeout(5*time.Second), ztest.Timeout(time.Millisecond))

		// Second drain resumes: the first segment is not written again.
		_, err = ws.Write([]byte("BBB"))
		require.NoError(t, err)
		ctx2, cancel2 := drainLong(t)
		defer cancel2()
		require.NoError(t, ws.Drain(ctx2))
		assert.Equal(t, "AAABBB", g.String(),
			"resumed drain must not rewrite already-flushed data")
	})

	t.Run("writes during drain do not cross the boundary", func(t *testing.T) {
		g := newGateWriteSyncer(true, false)
		ws := &BufferedWriteSyncer{WS: g, Size: 3}
		defer func() {
			g.releaseWrites()
			assert.NoError(t, ws.Stop())
		}()

		_, err := ws.Write([]byte("pre"))
		require.NoError(t, err)

		ctx, cancel := drainSoon(t)
		drained := make(chan error, 1)
		go func() { drained <- ws.Drain(ctx) }()

		// Wait until the pre-boundary segment is in flight on the writer.
		select {
		case <-g.entered:
		case <-time.After(ztest.Timeout(5 * time.Second)):
			t.Fatal("drain never reached the underlying writer")
		}

		// A concurrent write is accepted without blocking, but it must not
		// be part of the ongoing drain.
		writeDone := make(chan struct{})
		go func() {
			_, _ = ws.Write([]byte("pos"))
			close(writeDone)
		}()
		select {
		case <-writeDone:
		case <-time.After(ztest.Timeout(5 * time.Second)):
			t.Fatal("writes must still be accepted while drain is blocked")
		}

		require.ErrorIs(t, <-drained, context.DeadlineExceeded)
		cancel()

		// Once the gate opens, only the pre-boundary segment is written by
		// the background operation; the post-boundary bytes stay buffered.
		g.releaseWrites()
		require.Eventually(t, func() bool {
			return g.String() == "pre"
		}, ztest.Timeout(5*time.Second), ztest.Timeout(time.Millisecond))

		// A follow-up drain picks the remaining segment up; nothing is
		// duplicated or lost.
		ctx2, cancel2 := drainLong(t)
		defer cancel2()
		require.NoError(t, ws.Drain(ctx2))
		assert.Equal(t, "prepos", g.String())
	})

	t.Run("deadline on underlying sync is resumable", func(t *testing.T) {
		buf := &bytes.Buffer{}
		g := newGateWriteSyncer(false, true)
		ws := &BufferedWriteSyncer{
			WS: NewMultiWriteSyncer(AddSync(buf), g),
		}
		defer func() {
			g.releaseSyncs()
			assert.NoError(t, ws.Stop())
		}()

		requireWriteWorks(t, ws)

		ctx, cancel := drainSoon(t)
		require.ErrorIs(t, ws.Drain(ctx), context.DeadlineExceeded)
		cancel()

		g.releaseSyncs()
		ctx2, cancel2 := drainLong(t)
		defer cancel2()
		require.NoError(t, ws.Drain(ctx2))
		assert.Equal(t, "foo", buf.String())
	})

	t.Run("already-canceled context does not flush", func(t *testing.T) {
		buf := &bytes.Buffer{}
		ws := &BufferedWriteSyncer{WS: AddSync(buf)}
		defer func() { assert.NoError(t, ws.Stop()) }()

		requireWriteWorks(t, ws)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.ErrorIs(t, ws.Drain(ctx), context.Canceled)
		assert.Empty(t, buf.String(), "canceled drain must not flush anything")
	})

	t.Run("propagates write errors", func(t *testing.T) {
		ws := &BufferedWriteSyncer{WS: &ztest.FailWriter{}}
		_, err := ws.Write([]byte("foo"))
		require.NoError(t, err)
		assert.Error(t, ws.Drain(context.Background()))

		// Newly accepted data is retried on the next flush and fails again.
		_, err = ws.Write([]byte("bar"))
		require.NoError(t, err)
		assert.Error(t, ws.Stop())
		assert.NoError(t, ws.Stop())
	})

	t.Run("without being started", func(t *testing.T) {
		syncer := &ztest.Buffer{}
		ws := &BufferedWriteSyncer{WS: syncer}
		assert.NoError(t, ws.Drain(context.Background()))
		assert.True(t, syncer.Called(), "expected underlying Sync to still run")
		assert.NoError(t, ws.Stop(), "Stop must not fail")
	})

	t.Run("after stop", func(t *testing.T) {
		ws := &BufferedWriteSyncer{WS: AddSync(new(bytes.Buffer))}
		require.NoError(t, ws.Stop())
		assert.NoError(t, ws.Drain(context.Background()))
	})
}

// TestBufferedWriteSyncerDrainKeepsSyncSemantics verifies that a regular Sync
// still flushes everything buffered at call time, including writes that
// arrived after a drain boundary.
func TestBufferedWriteSyncerDrainKeepsSyncSemantics(t *testing.T) {
	buf := &bytes.Buffer{}
	ws := &BufferedWriteSyncer{WS: AddSync(buf), Size: 3}
	defer func() { assert.NoError(t, ws.Stop()) }()

	_, err := ws.Write([]byte("pre"))
	require.NoError(t, err)
	ctx, cancel := drainLong(t)
	defer cancel()
	require.NoError(t, ws.Drain(ctx))
	require.Equal(t, "pre", buf.String())

	_, err = ws.Write([]byte("post"))
	require.NoError(t, err)
	require.NoError(t, ws.Sync(), "Sync must keep flushing everything")
	assert.Equal(t, "prepost", buf.String())
}

// TestBufferedWriteSyncerDrainConcurrentWrites stresses the drain boundary:
// data is accepted concurrently with a blocked, timed-out drain, and nothing
// is lost when the writer recovers.
func TestBufferedWriteSyncerDrainConcurrentWrites(t *testing.T) {
	g := newGateWriteSyncer(true, false)
	ws := &BufferedWriteSyncer{WS: g}
	defer func() {
		g.releaseWrites()
		assert.NoError(t, ws.Stop())
	}()

	const (
		goroutines = 16
		messages   = 100
		msg        = "0123456789"
	)

	// Accept a first write so the drain has pre-boundary data to block on.
	_, err := ws.Write([]byte(msg))
	require.NoError(t, err)

	ctx, cancel := drainTimeout(30 * time.Millisecond)
	drained := make(chan error, 1)
	go func() { drained <- ws.Drain(ctx) }()

	// Wait for the first segment to be in flight before writers start.
	waitForSignal(t, g.entered, "first drain write")

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < messages; j++ {
				_, _ = ws.Write([]byte(msg))
			}
		}()
	}
	wg.Wait()

	require.ErrorIs(t, <-drained, context.DeadlineExceeded)
	cancel()

	g.releaseWrites()
	require.NoError(t, ws.Stop())
	assert.Len(t, g.String(), (goroutines*messages+1)*len(msg),
		"all accepted writes must survive the timed-out drain")
}

var _ DrainingWriteSyncer = (*BufferedWriteSyncer)(nil)
