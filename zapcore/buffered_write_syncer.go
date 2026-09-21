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
	"context"
	"io"
	"sync"
	"time"

	"go.uber.org/multierr"
)

const (
	// _defaultBufferSize specifies the default size used by Buffer.
	_defaultBufferSize = 256 * 1024 // 256 kB

	// _defaultFlushInterval specifies the default flush interval for
	// Buffer.
	_defaultFlushInterval = 30 * time.Second
)

// A BufferedWriteSyncer is a WriteSyncer that buffers writes in-memory before
// flushing them to a wrapped WriteSyncer after reaching some limit, or at some
// fixed interval--whichever comes first.
//
// BufferedWriteSyncer is safe for concurrent use. You don't need to use
// zapcore.Lock for WriteSyncers with BufferedWriteSyncer.
//
// To set up a BufferedWriteSyncer, construct a WriteSyncer for your log
// destination (*os.File is a valid WriteSyncer), wrap it with
// BufferedWriteSyncer, and defer a Stop() call for when you no longer need the
// object.
//
//	 func main() {
//	   ws := ... // your log destination
//	   bws := &zapcore.BufferedWriteSyncer{WS: ws}
//	   defer bws.Stop()
//
//	   // ...
//	   core := zapcore.NewCore(enc, bws, lvl)
//	   logger := zap.New(core)
//
//	   // ...
//	}
//
// By default, a BufferedWriteSyncer will buffer up to 256 kilobytes of logs,
// waiting at most 30 seconds between flushes.
// You can customize these parameters by setting the Size or FlushInterval
// fields.
// For example, the following buffers up to 512 kB of logs before flushing them
// to Stderr, with a maximum of one minute between each flush.
//
//	ws := &BufferedWriteSyncer{
//	  WS:            os.Stderr,
//	  Size:          512 * 1024, // 512 kB
//	  FlushInterval: time.Minute,
//	}
//	defer ws.Stop()
//
// For graceful shutdown with a hard deadline, use Drain instead of Sync:
// it flushes only the data that had been accepted before the call, honors
// the context deadline, and can be called again to resume flushing whatever
// a previous, timed-out call did not finish.
type BufferedWriteSyncer struct {
	// WS is the WriteSyncer around which BufferedWriteSyncer will buffer
	// writes.
	//
	// This field is required.
	WS WriteSyncer

	// Size specifies the maximum amount of data the writer will buffered
	// before flushing.
	//
	// Defaults to 256 kB if unspecified.
	Size int

	// FlushInterval specifies how often the writer should flush data if
	// there have been no writes.
	//
	// Defaults to 30 seconds if unspecified.
	FlushInterval time.Duration

	// Clock, if specified, provides control of the source of time for the
	// writer.
	//
	// Defaults to the system clock.
	Clock Clock

	// mu protects the in-memory buffer state below. Critical sections only
	// manipulate memory; physical writes are performed without mu held.
	mu          sync.Mutex
	initialized bool // whether initialize() has run
	stopped     bool // whether Stop() has run
	active      []byte
	queued      [][]byte // sealed segments awaiting a physical write, FIFO
	inflight    *inflightWrite
	ticker      *time.Ticker
	stop        chan struct{} // closed when flushLoop should stop
	done        chan struct{} // closed when flushLoop has stopped

	// flushMu serializes coordinators of physical writes (Drain, Sync, and
	// the synchronous overflow flush in Write), so the wrapped WriteSyncer
	// never receives concurrent Write calls.
	flushMu sync.Mutex
}

// inflightWrite is a segment being written by a goroutine that may outlive
// the Drain or Sync call that started it when a deadline was reached.
type inflightWrite struct {
	seg  []byte
	done chan struct{}
	err  error
}

func (s *BufferedWriteSyncer) initialize() {
	flushInterval := s.FlushInterval
	if flushInterval == 0 {
		flushInterval = _defaultFlushInterval
	}

	if s.Clock == nil {
		s.Clock = DefaultClock
	}

	s.ticker = s.Clock.NewTicker(flushInterval)
	s.stop = make(chan struct{})
	s.done = make(chan struct{})
	s.initialized = true
	go s.flushLoop()
}

func (s *BufferedWriteSyncer) bufferSize() int {
	if s.Size > 0 {
		return s.Size
	}
	return _defaultBufferSize
}

// Write writes log data into buffer syncer directly, multiple Write calls will be batched,
// and log data will be flushed to disk when the buffer is full or periodically.
func (s *BufferedWriteSyncer) Write(bs []byte) (int, error) {
	s.mu.Lock()

	if !s.initialized {
		s.initialize()
	}

	size := s.bufferSize()

	// To avoid partial writes from being flushed, seal the active segment if:
	// * the current write doesn't fit into it, and
	// * it is not empty (an oversized write on an empty active segment gets
	//   its own segment instead).
	sealed := false
	if len(bs) > size-len(s.active) && len(s.active) > 0 {
		s.queued = append(s.queued, s.active)
		s.active = nil
		sealed = true
	}

	if len(bs) > size {
		// The Write contract lets callers reuse bs, so copy an oversized
		// write into its own segment rather than retaining the input.
		seg := make([]byte, len(bs))
		copy(seg, bs)
		s.queued = append(s.queued, seg)
		sealed = true
	} else {
		if cap(s.active) == 0 {
			s.active = make([]byte, 0, size)
		}
		s.active = append(s.active, bs...)
	}

	if !sealed {
		s.mu.Unlock()
		return len(bs), nil
	}

	// Flush synchronously, preserving the historical overflow behavior:
	// all currently queued segments become visible before Write returns.
	pending := len(s.queued)
	if s.inflight != nil {
		pending++
	}
	s.mu.Unlock()

	if err := s.driveSegments(context.Background(), pending); err != nil {
		return 0, err
	}
	return len(bs), nil
}

// Sync flushes buffered log data into disk directly.
func (s *BufferedWriteSyncer) Sync() error {
	s.mu.Lock()
	if !s.initialized {
		s.mu.Unlock()
		return s.WS.Sync()
	}
	pending := s.sealActiveLocked()
	s.mu.Unlock()

	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	err := s.drive(context.Background(), pending)
	return multierr.Append(err, s.WS.Sync())
}

// Drain flushes the log data already accepted at the time of the call into
// the wrapped WriteSyncer, honoring ctx as a hard deadline. It is intended for
// graceful shutdown:
//
//   - Data written after Drain starts is not flushed by this call; those
//     writes stay buffered and cross no drain boundary.
//   - If the deadline expires while a write or sync to the wrapped
//     WriteSyncer is in flight, Drain returns the context error while that
//     operation completes in the background.
//   - Calling Drain again (typically with a fresh context) resumes the
//     unfinished work; already-flushed data is never written twice.
//   - When the wrapped WriteSyncer is a multi-writer (or itself a
//     DrainingWriteSyncer), underlying outputs are drained in parallel.
//
// Sync keeps its original, unbounded semantics and is unaffected.
func (s *BufferedWriteSyncer) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.Lock()
	if !s.initialized {
		s.mu.Unlock()
		return s.syncUnderlying(ctx)
	}
	// Establish the drain boundary: seal the active segment. Everything
	// appended to queued after this point is younger than this Drain.
	pending := s.sealActiveLocked()
	s.mu.Unlock()

	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	err := s.drive(ctx, pending)
	// If the deadline was reached, don't start another potentially blocking
	// operation on the underlying writer; it will be covered by the next
	// Drain.
	if ctx.Err() == nil {
		err = multierr.Append(err, s.syncUnderlying(ctx))
	}
	return err
}

// sealActiveLocked moves the active segment into the queue and returns the
// number of segments that still need a physical write (the in-flight one, if
// any, plus everything queued). Caller holds mu.
func (s *BufferedWriteSyncer) sealActiveLocked() int {
	if len(s.active) > 0 {
		s.queued = append(s.queued, s.active)
		s.active = nil
	}
	pending := len(s.queued)
	if s.inflight != nil {
		pending++
	}
	return pending
}

// syncUnderlying flushes/syncs the wrapped WriteSyncer, taking its parallel
// drain capability into account and honoring the deadline on the caller's
// side. Caller holds flushMu.
func (s *BufferedWriteSyncer) syncUnderlying(ctx context.Context) error {
	if d, ok := s.WS.(DrainingWriteSyncer); ok {
		return d.Drain(ctx)
	}
	return awaitCall(ctx, s.WS.Sync)
}

// driveSegments runs a single drive pass under flushMu.
func (s *BufferedWriteSyncer) driveSegments(ctx context.Context, pending int) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	return s.drive(ctx, pending)
}

// drive writes pending queued segments, in order, to the wrapped WriteSyncer.
// It waits for a segment left in flight by a previous timed-out call before
// launching new ones, so timed-out calls can be resumed and no segment is
// written twice. When ctx is canceled, it returns immediately; the in-flight
// write keeps running in the background and a later call resumes here.
//
// Caller holds flushMu; mu is only taken for short bookkeeping sections.
func (s *BufferedWriteSyncer) drive(ctx context.Context, pending int) error {
	for pending > 0 {
		s.mu.Lock()
		if inf := s.inflight; inf != nil {
			s.mu.Unlock()

			select {
			case <-inf.done:
				pending--
				if inf.err != nil {
					// Any unwritten remainder was requeued by the writer.
					return inf.err
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if len(s.queued) == 0 {
			s.mu.Unlock()
			return nil
		}

		seg := s.queued[0]
		s.queued = s.queued[1:]
		inf := &inflightWrite{
			seg:  seg,
			done: make(chan struct{}),
		}
		s.inflight = inf
		s.mu.Unlock()

		s.startSegmentWrite(inf)

		select {
		case <-inf.done:
			pending--
			if inf.err != nil {
				return inf.err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// startSegmentWrite writes a single segment in a detached goroutine so that a
// blocked underlying writer cannot pin the Drain or Sync caller past its
// deadline. On exit it clears the in-flight slot; if only part of the segment
// was written, the unwritten remainder is requeued at the head for retry.
func (s *BufferedWriteSyncer) startSegmentWrite(inf *inflightWrite) {
	go func() {
		n, err := writeAll(s.WS, inf.seg)

		s.mu.Lock()
		if n < len(inf.seg) {
			rem := make([]byte, len(inf.seg)-n)
			copy(rem, inf.seg[n:])
			s.queued = append([][]byte{rem}, s.queued...)
		}
		inf.err = err
		if s.inflight == inf {
			s.inflight = nil
		}
		s.mu.Unlock()

		close(inf.done)
	}()
}

// writeAll writes the full p to w, advancing through short writes.
func writeAll(w io.Writer, p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n, err := w.Write(p)
		total += n
		if err != nil {
			return total, err
		}
		if n <= 0 || n > len(p) {
			return total, io.ErrShortWrite
		}
		p = p[n:]
	}
	return total, nil
}

// flushLoop flushes the buffer at the configured interval until Stop is
// called.
func (s *BufferedWriteSyncer) flushLoop() {
	defer close(s.done)

	for {
		select {
		case <-s.ticker.C:
			// we just simply ignore error here
			// because any write error is returned again by later Sync
			// and Stop calls, e.g. as part of the close
			_ = s.Sync()
		case <-s.stop:
			return
		}
	}
}

// Stop closes the buffer, cleans up background goroutines, and flushes
// remaining unwritten data.
func (s *BufferedWriteSyncer) Stop() (err error) {
	// Critical section.
	stopped := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		if !s.initialized {
			return false
		}

		if s.stopped {
			return false
		}
		s.stopped = true

		s.ticker.Stop()
		close(s.stop) // tell flushLoop to stop
		return true
	}()

	// Not initialized, or already stopped, no need for any cleanup.
	if !stopped {
		return
	}

	// Wait for flushLoop to end outside of the lock, as it may need the lock to complete.
	// See https://github.com/uber-go/zap/issues/1428 for details.
	<-s.done

	return s.Sync()
}
