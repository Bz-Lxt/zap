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
	"fmt"
	"runtime"
	"strings"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap/internal/bufferpool"
	"go.uber.org/zap/internal/exit"
	"go.uber.org/zap/internal/pool"
)

var _cePool = pool.New(func() *CheckedEntry {
	// Pre-allocate some space for cores and fan-out branches.
	return &CheckedEntry{
		cores:    make([]Core, 4),
		branches: make([]coreBranch, 4),
	}
})

func getCheckedEntry() *CheckedEntry {
	ce := _cePool.Get()
	ce.reset()
	return ce
}

func putCheckedEntry(ce *CheckedEntry) {
	if ce == nil {
		return
	}
	_cePool.Put(ce)
}

// NewEntryCaller makes an EntryCaller from the return signature of
// runtime.Caller.
func NewEntryCaller(pc uintptr, file string, line int, ok bool) EntryCaller {
	if !ok {
		return EntryCaller{}
	}
	return EntryCaller{
		PC:      pc,
		File:    file,
		Line:    line,
		Defined: true,
	}
}

// EntryCaller represents the caller of a logging function.
type EntryCaller struct {
	Defined  bool
	PC       uintptr
	File     string
	Line     int
	Function string
}

// String returns the full path and line number of the caller.
func (ec EntryCaller) String() string {
	return ec.FullPath()
}

// FullPath returns a /full/path/to/package/file:line description of the
// caller.
func (ec EntryCaller) FullPath() string {
	if !ec.Defined {
		return "undefined"
	}
	buf := bufferpool.Get()
	buf.AppendString(ec.File)
	buf.AppendByte(':')
	buf.AppendInt(int64(ec.Line))
	caller := buf.String()
	buf.Free()
	return caller
}

// TrimmedPath returns a package/file:line description of the caller,
// preserving only the leaf directory name and file name.
func (ec EntryCaller) TrimmedPath() string {
	if !ec.Defined {
		return "undefined"
	}
	// nb. To make sure we trim the path correctly on Windows too, we
	// counter-intuitively need to use '/' and *not* os.PathSeparator here,
	// because the path given originates from Go stdlib, specifically
	// runtime.Caller() which (as of Mar/17) returns forward slashes even on
	// Windows.
	//
	// See https://github.com/golang/go/issues/3335
	// and https://github.com/golang/go/issues/18151
	//
	// for discussion on the issue on Go side.
	//
	// Find the last separator.
	//
	idx := strings.LastIndexByte(ec.File, '/')
	if idx == -1 {
		return ec.FullPath()
	}
	// Find the penultimate separator.
	idx = strings.LastIndexByte(ec.File[:idx], '/')
	if idx == -1 {
		return ec.FullPath()
	}
	buf := bufferpool.Get()
	// Keep everything after the penultimate separator.
	buf.AppendString(ec.File[idx+1:])
	buf.AppendByte(':')
	buf.AppendInt(int64(ec.Line))
	caller := buf.String()
	buf.Free()
	return caller
}

// An Entry represents a complete log message. The entry's structured context
// is already serialized, but the log level, time, message, and call site
// information are available for inspection and modification. Any fields left
// empty will be omitted when encoding.
//
// Entries are pooled, so any functions that accept them MUST be careful not to
// retain references to them.
type Entry struct {
	Level      Level
	Time       time.Time
	LoggerName string
	Message    string
	Caller     EntryCaller
	Stack      string
}

// CheckWriteHook is a custom action that may be executed after an entry is
// written.
//
// Register one on a CheckedEntry with the After method.
//
//	if ce := logger.Check(...); ce != nil {
//	  ce = ce.After(hook)
//	  ce.Write(...)
//	}
//
// You can configure the hook for Fatal log statements at the logger level with
// the zap.WithFatalHook option.
type CheckWriteHook interface {
	// OnWrite is invoked with the CheckedEntry that was written and a list
	// of fields added with that entry.
	//
	// The list of fields DOES NOT include fields that were already added
	// to the logger with the With method.
	OnWrite(*CheckedEntry, []Field)
}

// CheckWriteAction indicates what action to take after a log entry is
// processed. Actions are ordered in increasing severity.
type CheckWriteAction uint8

const (
	// WriteThenNoop indicates that nothing special needs to be done. It's the
	// default behavior.
	WriteThenNoop CheckWriteAction = iota
	// WriteThenGoexit runs runtime.Goexit after Write.
	WriteThenGoexit
	// WriteThenPanic causes a panic after Write.
	WriteThenPanic
	// WriteThenFatal causes an os.Exit(1) after Write.
	WriteThenFatal
)

// OnWrite implements the OnWrite method to keep CheckWriteAction compatible
// with the new CheckWriteHook interface which deprecates CheckWriteAction.
func (a CheckWriteAction) OnWrite(ce *CheckedEntry, _ []Field) {
	switch a {
	case WriteThenGoexit:
		runtime.Goexit()
	case WriteThenPanic:
		panic(ce.Message)
	case WriteThenFatal:
		exit.With(1)
	}
}

var _ CheckWriteHook = CheckWriteAction(0)

// CheckPreWriteHook is a function that transforms an Entry and its Fields
// before they are written to cores. Register one on a CheckedEntry with the
// Before method.
//
// Pre-write hooks run in the order they were added, before any Core's Write
// method is called. They may modify the Entry and Fields freely.
type CheckPreWriteHook func(Entry, []Field) (Entry, []Field)

// WriteStatus describes the outcome of delivering an Entry to a single
// fan-out branch, i.e. one of the Cores offered the Entry during Check.
type WriteStatus uint8

const (
	// WriteNotSelected means the branch was declared during Check but did
	// not select the Entry, so its Write method was not called.
	WriteNotSelected WriteStatus = iota
	// WriteSucceeded means the branch selected the Entry and its Write
	// method returned no error.
	WriteSucceeded
	// WriteFailed means the branch selected the Entry but its Write method
	// returned an error.
	WriteFailed
)

// WriteResult reports the delivery result of a single declared fan-out
// branch.
type WriteResult struct {
	// Core is the branch that this result describes.
	Core Core
	// Status is the delivery status of the branch.
	Status WriteStatus
	// Err holds the error returned by the branch when Status is WriteFailed.
	// It is nil otherwise.
	Err error
}

// CheckPostWriteHook is a callback invoked after an Entry has been delivered
// to the Cores that selected it during Check. Register one on a CheckedEntry
// with the AfterWrite method.
//
// The hook receives the Entry and fields that were written and the delivery
// result of every declared branch in branch declaration order: branches that
// selected the Entry report WriteSucceeded or WriteFailed, and branches that
// did not select it report WriteNotSelected. A failure on one branch never
// masks the results of the other branches.
//
// Hooks run in the order they were added, before any terminal CheckWriteHook
// registered with After. An error returned by the hook is written to the
// CheckedEntry's ErrorOutput and does not keep subsequent hooks or the
// terminal action from running. The hook may safely log again.
type CheckPostWriteHook func(Entry, []Field, []WriteResult) error

// CheckedEntry is an Entry together with a collection of Cores that have
// already agreed to log it.
//
// CheckedEntry references should be created by calling AddCore, DeclareCore,
// Before, AfterWrite, or After on a nil *CheckedEntry. References are
// returned to a pool after Write, and MUST NOT be retained after calling
// their Write method.
type CheckedEntry struct {
	Entry
	ErrorOutput WriteSyncer
	dirty       bool // best-effort detection of pool misuse
	after       CheckWriteHook
	cores       []Core
	before      []CheckPreWriteHook
	afterWrite  []CheckPostWriteHook
	branches    []coreBranch
	openBranch  int // index of the most recently declared open branch, or -1
}

// coreBranch records a fan-out branch declared during Check and the range of
// entries it caused to be added to the cores slice. An empty range means the
// branch did not select the Entry.
type coreBranch struct {
	core   Core
	start  int
	end    int
	parent int // index of the enclosing open branch, or -1
}

func (ce *CheckedEntry) reset() {
	ce.Entry = Entry{}
	ce.ErrorOutput = nil
	ce.dirty = false
	ce.after = nil
	for i := range ce.cores {
		// don't keep references to cores
		ce.cores[i] = nil
	}
	ce.cores = ce.cores[:0]
	for i := range ce.before {
		ce.before[i] = nil
	}
	ce.before = ce.before[:0]
	for i := range ce.afterWrite {
		ce.afterWrite[i] = nil
	}
	ce.afterWrite = ce.afterWrite[:0]
	for i := range ce.branches {
		ce.branches[i].core = nil
	}
	ce.branches = ce.branches[:0]
	ce.openBranch = -1
}

// Write writes the entry to the stored Cores, returns any errors, and returns
// the CheckedEntry reference to a pool for immediate re-use. Finally, it
// executes any required CheckWriteAction.
func (ce *CheckedEntry) Write(fields ...Field) {
	if ce == nil {
		return
	}

	if ce.dirty {
		if ce.ErrorOutput != nil {
			// Make a best effort to detect unsafe re-use of this CheckedEntry.
			// If the entry is dirty, log an internal error; because the
			// CheckedEntry is being used after it was returned to the pool,
			// the message may be an amalgamation from multiple call sites.
			_, _ = fmt.Fprintf(
				ce.ErrorOutput,
				"%v Unsafe CheckedEntry re-use near Entry %+v.\n",
				ce.Time,
				ce.Entry,
			)
			_ = ce.ErrorOutput.Sync() // ignore error
		}
		return
	}
	ce.dirty = true
	// Return the entry to the pool no matter how Write unwinds: a hook may
	// return an error, log again, panic, or terminate the goroutine.
	defer putCheckedEntry(ce)

	ent := ce.Entry
	for i := range ce.before {
		ent, fields = ce.before[i](ent, fields)
	}

	// Deliver to every selected branch and record each result separately, so
	// a failure on one branch does not mask the results of the others. The
	// per-core errors are only collected when a post-write hook will observe
	// them, to keep the common hook-free path allocation-free.
	var err error
	var writeErrs []error
	if len(ce.afterWrite) > 0 {
		writeErrs = make([]error, len(ce.cores))
	}
	for i := range ce.cores {
		writeErr := ce.cores[i].Write(ent, fields)
		if writeErrs != nil {
			writeErrs[i] = writeErr
		}
		err = multierr.Append(err, writeErr)
	}
	if err != nil && ce.ErrorOutput != nil {
		_, _ = fmt.Fprintf(
			ce.ErrorOutput,
			"%v write error: %v\n",
			ce.Time,
			err,
		)
		_ = ce.ErrorOutput.Sync() // ignore error
	}

	// Post-write hooks observe the real per-branch delivery results in
	// branch declaration order, including branches that did not select the
	// entry. Hook errors are reported but do not stop the wrap-up.
	if len(ce.afterWrite) > 0 {
		var results []WriteResult
		if len(ce.branches) > 0 {
			results = make([]WriteResult, len(ce.branches))
			for i := range ce.branches {
				results[i] = ce.branches[i].result(writeErrs)
			}
		} else {
			// Cores registered outside of a DeclareCore/CloseCore walk are
			// each their own selected branch.
			results = make([]WriteResult, len(ce.cores))
			for i := range ce.cores {
				r := WriteResult{Core: ce.cores[i], Err: writeErrs[i]}
				if writeErrs[i] == nil {
					r.Status = WriteSucceeded
				} else {
					r.Status = WriteFailed
				}
				results[i] = r
			}
		}
		for i := range ce.afterWrite {
			if hookErr := ce.afterWrite[i](ent, fields, results); hookErr != nil && ce.ErrorOutput != nil {
				_, _ = fmt.Fprintf(
					ce.ErrorOutput,
					"%v post-write hook error: %v\n",
					ce.Time,
					hookErr,
				)
				_ = ce.ErrorOutput.Sync() // ignore error
			}
		}
	}

	// Terminal behavior runs last, after all post-write processing.
	hook := ce.after
	if hook != nil {
		hook.OnWrite(ce, fields)
	}
}

// result builds the WriteResult for a declared branch from the per-core
// errors collected during Write.
func (b coreBranch) result(writeErrs []error) WriteResult {
	r := WriteResult{Core: b.core}
	if b.end <= b.start {
		r.Status = WriteNotSelected
		return r
	}
	var err error
	for i := b.start; i < b.end; i++ {
		err = multierr.Append(err, writeErrs[i])
	}
	r.Err = err
	if err == nil {
		r.Status = WriteSucceeded
	} else {
		r.Status = WriteFailed
	}
	return r
}

// AddCore adds a Core that has agreed to log this CheckedEntry. It's intended to be
// used by Core.Check implementations, and is safe to call on nil CheckedEntry
// references.
func (ce *CheckedEntry) AddCore(ent Entry, core Core) *CheckedEntry {
	if ce == nil {
		ce = getCheckedEntry()
		ce.Entry = ent
	}
	ce.cores = append(ce.cores, core)
	return ce
}

// DeclareCore reports that the given fan-out branch Core is about to be
// offered the Entry during a Check walk. It is intended for fan-out Core
// implementations, such as the Core returned by NewTee. Each call must be
// followed by the branch's Check call and a matching CloseCore call:
//
//	ce = ce.DeclareCore(ent, branch)
//	ce = branch.Check(ent, ce)
//	ce = ce.CloseCore()
//
// A branch whose Check adds no Cores is reported with WriteNotSelected to
// post-write hooks. It is safe to call on nil CheckedEntry references.
func (ce *CheckedEntry) DeclareCore(ent Entry, core Core) *CheckedEntry {
	if ce == nil {
		ce = getCheckedEntry()
		ce.Entry = ent
	}
	ce.branches = append(ce.branches, coreBranch{
		core:   core,
		start:  len(ce.cores),
		parent: ce.openBranch,
	})
	ce.openBranch = len(ce.branches) - 1
	return ce
}

// CloseCore finishes the most recently declared branch that is still open,
// after that branch's Check call has returned. Like AddCore, it is intended
// for fan-out Core implementations and is safe to call on nil CheckedEntry
// references.
func (ce *CheckedEntry) CloseCore() *CheckedEntry {
	i := ce.openBranch
	if i < 0 {
		return ce
	}
	ce.branches[i].end = len(ce.cores)
	ce.openBranch = ce.branches[i].parent
	return ce
}

// Should sets this CheckedEntry's CheckWriteAction, which controls whether a
// Core will panic or fatal after writing this log entry. Like AddCore, it's
// safe to call on nil CheckedEntry references.
//
// Deprecated: Use [CheckedEntry.After] instead.
func (ce *CheckedEntry) Should(ent Entry, should CheckWriteAction) *CheckedEntry {
	return ce.After(ent, should)
}

// Before adds a pre-write hook that transforms the Entry and Fields before
// they are written to any registered Cores. Multiple hooks run in the order
// they were added. It's safe to call this on nil CheckedEntry references.
func (ce *CheckedEntry) Before(ent Entry, hook CheckPreWriteHook) *CheckedEntry {
	if ce == nil {
		ce = getCheckedEntry()
		ce.Entry = ent
	}
	ce.before = append(ce.before, hook)
	return ce
}

// AfterWrite adds a post-write hook that is invoked after the Entry is
// delivered to the Cores that selected it. The hook sees every declared
// branch's delivery result in declaration order. Multiple hooks run in the
// order they were added, and all of them run before the terminal hook set
// with After. It's safe to call this on nil CheckedEntry references.
func (ce *CheckedEntry) AfterWrite(ent Entry, hook CheckPostWriteHook) *CheckedEntry {
	if ce == nil {
		ce = getCheckedEntry()
		ce.Entry = ent
	}
	ce.afterWrite = append(ce.afterWrite, hook)
	return ce
}

// After sets this CheckEntry's CheckWriteHook, which will be called after this
// log entry has been written. It's safe to call this on nil CheckedEntry
// references.
func (ce *CheckedEntry) After(ent Entry, hook CheckWriteHook) *CheckedEntry {
	if ce == nil {
		ce = getCheckedEntry()
		ce.Entry = ent
	}
	ce.after = hook
	return ce
}
