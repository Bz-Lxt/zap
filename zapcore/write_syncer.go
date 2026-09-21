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
	"context"
	"io"
	"sync"

	"go.uber.org/multierr"
)

// A WriteSyncer is an io.Writer that can also flush any buffered data. Note
// that *os.File (and thus, os.Stderr and os.Stdout) implement WriteSyncer.
type WriteSyncer interface {
	io.Writer
	Sync() error
}

// A DrainingWriteSyncer is a WriteSyncer that can flush any buffered data
// within a caller-specified deadline.
//
// Drain is intended for graceful shutdown: it flushes data already accepted
// by the WriteSyncer, returning early when ctx is canceled. A write or sync
// already in flight cannot be interrupted mid-operation and may complete in
// the background; calling Drain again with a new context resumes the work
// that was not finished. Data accepted after a Drain call starts must not be
// flushed by that call.
type DrainingWriteSyncer interface {
	WriteSyncer

	// Drain flushes any buffered data, returning when all eligible data has
	// been flushed, or when ctx is canceled.
	Drain(ctx context.Context) error
}

// awaitCall invokes fn, returning ctx.Err() if ctx is canceled before fn
// returns. In that case fn keeps running in the background.
func awaitCall(ctx context.Context, fn func() error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	done := make(chan error, 1)
	go func() {
		done <- fn()
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AddSync converts an io.Writer to a WriteSyncer. It attempts to be
// intelligent: if the concrete type of the io.Writer implements WriteSyncer,
// we'll use the existing Sync method. If it doesn't, we'll add a no-op Sync.
func AddSync(w io.Writer) WriteSyncer {
	switch w := w.(type) {
	case WriteSyncer:
		return w
	default:
		return writerWrapper{w}
	}
}

type lockedWriteSyncer struct {
	sync.Mutex
	ws WriteSyncer
}

// Lock wraps a WriteSyncer in a mutex to make it safe for concurrent use. In
// particular, *os.Files must be locked before use.
func Lock(ws WriteSyncer) WriteSyncer {
	if _, ok := ws.(*lockedWriteSyncer); ok {
		// no need to layer on another lock
		return ws
	}
	return &lockedWriteSyncer{ws: ws}
}

func (s *lockedWriteSyncer) Write(bs []byte) (int, error) {
	s.Lock()
	n, err := s.ws.Write(bs)
	s.Unlock()
	return n, err
}

func (s *lockedWriteSyncer) Sync() error {
	s.Lock()
	err := s.ws.Sync()
	s.Unlock()
	return err
}

// Drain serializes the call with Write and Sync, delegating to the wrapped
// WriteSyncer's Drain when it supports bounded draining. A plain Sync is
// invoked with the deadline honored on the caller's side. If the deadline
// expires while the call is still in flight, it completes in the background
// while still holding the lock, so serialization with Write is preserved.
func (s *lockedWriteSyncer) Drain(ctx context.Context) error {
	return awaitCall(ctx, func() error {
		s.Lock()
		defer s.Unlock()

		if d, ok := s.ws.(DrainingWriteSyncer); ok {
			return d.Drain(ctx)
		}
		return s.ws.Sync()
	})
}

type writerWrapper struct {
	io.Writer
}

func (w writerWrapper) Sync() error {
	return nil
}

type multiWriteSyncer []WriteSyncer

// NewMultiWriteSyncer creates a WriteSyncer that duplicates its writes
// and sync calls, much like io.MultiWriter.
func NewMultiWriteSyncer(ws ...WriteSyncer) WriteSyncer {
	if len(ws) == 1 {
		return ws[0]
	}
	return multiWriteSyncer(ws)
}

// See https://golang.org/src/io/multi.go
// When not all underlying syncers write the same number of bytes,
// the smallest number is returned even though Write() is called on
// all of them.
func (ws multiWriteSyncer) Write(p []byte) (int, error) {
	var writeErr error
	nWritten := 0
	for _, w := range ws {
		n, err := w.Write(p)
		writeErr = multierr.Append(writeErr, err)
		if nWritten == 0 && n != 0 {
			nWritten = n
		} else if n < nWritten {
			nWritten = n
		}
	}
	return nWritten, writeErr
}

func (ws multiWriteSyncer) Sync() error {
	var err error
	for _, w := range ws {
		err = multierr.Append(err, w.Sync())
	}
	return err
}

// Drain flushes every underlying WriteSyncer concurrently, rather than
// sequentially like Sync. It returns when all of them have completed, or when
// ctx is canceled; in the latter case any still-running drain completes in the
// background, and a subsequent Drain resumes the unfinished work.
//
// All underlying WriteSyncers are called even if some of them fail, and their
// errors are combined.
func (ws multiWriteSyncer) Drain(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var (
		wg      sync.WaitGroup
		errMu   sync.Mutex
		syncErr error
	)
	for _, w := range ws {
		wg.Add(1)
		go func(w WriteSyncer) {
			defer wg.Done()

			var err error
			if d, ok := w.(DrainingWriteSyncer); ok {
				err = d.Drain(ctx)
			} else {
				err = w.Sync()
			}
			if err != nil {
				errMu.Lock()
				syncErr = multierr.Append(syncErr, err)
				errMu.Unlock()
			}
		}(w)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return syncErr
	case <-ctx.Done():
		// syncErr must not be read here: child goroutines may still be
		// appending to it. A subsequent Drain observes their results.
		return ctx.Err()
	}
}
