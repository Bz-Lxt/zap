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
	"sync"

	"go.uber.org/zap/internal/bufferpool"
)

// Core is a minimal, fast logger interface. It's designed for library authors
// to wrap in a more user-friendly API.
type Core interface {
	LevelEnabler

	// With adds structured context to the Core.
	With([]Field) Core
	// Check determines whether the supplied Entry should be logged (using the
	// embedded LevelEnabler and possibly some extra logic). If the entry
	// should be logged, the Core adds itself to the CheckedEntry and returns
	// the result.
	//
	// Callers must use Check before calling Write.
	Check(Entry, *CheckedEntry) *CheckedEntry
	// Write serializes the Entry and any Fields supplied at the log site and
	// writes them to their destination.
	//
	// If called, Write should always log the Entry and Fields; it should not
	// replicate the logic of Check.
	Write(Entry, []Field) error
	// Sync flushes buffered logs (if any).
	Sync() error
}

type nopCore struct{}

// NewNopCore returns a no-op Core.
func NewNopCore() Core                                        { return nopCore{} }
func (nopCore) Enabled(Level) bool                            { return false }
func (n nopCore) With([]Field) Core                           { return n }
func (nopCore) Check(_ Entry, ce *CheckedEntry) *CheckedEntry { return ce }
func (nopCore) Write(Entry, []Field) error                    { return nil }
func (nopCore) Sync() error                                   { return nil }

// NewCore creates a Core that writes logs to a WriteSyncer.
func NewCore(enc Encoder, ws WriteSyncer, enab LevelEnabler) Core {
	return &ioCore{
		LevelEnabler: enab,
		enc:          enc,
		out:          ws,
		batchMu:      &sync.Mutex{},
	}
}

type ioCore struct {
	LevelEnabler
	enc Encoder
	out WriteSyncer
	// batchMu serializes batch writes to this output, including retries, so
	// that concurrent batches never interleave. It is shared across Cores
	// derived with With, since they write to the same output.
	batchMu *sync.Mutex
}

var (
	_ Core           = (*ioCore)(nil)
	_ leveledEnabler = (*ioCore)(nil)
	_ BatchCore      = (*ioCore)(nil)
)

func (c *ioCore) Level() Level {
	return LevelOf(c.LevelEnabler)
}

func (c *ioCore) With(fields []Field) Core {
	clone := c.clone()
	addFields(clone.enc, fields)
	return clone
}

func (c *ioCore) Check(ent Entry, ce *CheckedEntry) *CheckedEntry {
	if c.Enabled(ent.Level) {
		return ce.AddCore(ent, c)
	}
	return ce
}

func (c *ioCore) Write(ent Entry, fields []Field) error {
	buf, err := c.enc.EncodeEntry(ent, fields)
	if err != nil {
		return err
	}
	_, err = c.out.Write(buf.Bytes())
	buf.Free()
	if err != nil {
		return err
	}
	if ent.Level > ErrorLevel {
		// Since we may be crashing the program, sync the output.
		// Ignore Sync errors, pending a clean solution to issue #370.
		_ = c.Sync()
	}
	return nil
}

// WriteBatch encodes all admitted entries into one staging buffer and writes
// them to the output with a single write (plus a sync when any entry is above
// ErrorLevel), so the records appear in the output contiguously and in order.
//
// If a write is only partially acknowledged, the write is retried with just
// the unacknowledged tail. A write that makes no progress aborts the batch
// and returns its error. The staging buffer is released in either case.
func (c *ioCore) WriteBatch(entries []BatchEntry) error {
	c.batchMu.Lock()
	defer c.batchMu.Unlock()

	staging := bufferpool.Get()
	defer staging.Free()

	syncAfter := false
	for i := range entries {
		buf, err := c.enc.EncodeEntry(entries[i].Entry, entries[i].Fields)
		if err != nil {
			return err
		}
		staging.AppendBytes(buf.Bytes())
		buf.Free()
		if entries[i].Level > ErrorLevel {
			syncAfter = true
		}
	}

	p := staging.Bytes()
	for len(p) > 0 {
		n, err := c.out.Write(p)
		if n > len(p) {
			n = len(p)
		}
		if err != nil {
			if n > 0 {
				// Some bytes were acknowledged; on recovery continue only
				// with the unacknowledged tail.
				p = p[n:]
				continue
			}
			return err
		}
		p = p[n:]
	}

	if syncAfter {
		return c.Sync()
	}
	return nil
}

func (c *ioCore) Sync() error {
	return c.out.Sync()
}

func (c *ioCore) clone() *ioCore {
	return &ioCore{
		LevelEnabler: c.LevelEnabler,
		enc:          c.enc.Clone(),
		out:          c.out,
		batchMu:      c.batchMu,
	}
}
