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

import "go.uber.org/multierr"

// BatchEntry pairs an Entry with the fields supplied at the log site for a
// single record in a batch. It is the batch analogue of the arguments passed
// to [Core.Write].
type BatchEntry struct {
	Entry
	Fields []Field
}

// BatchCore is implemented by Cores that can write multiple admitted entries
// with a single write to their output.
//
// Cores that do not implement BatchCore remain fully compatible with batch
// submissions: each admitted entry is written through a separate [Core.Write]
// call.
type BatchCore interface {
	Core

	// WriteBatch serializes every supplied entry (which has already passed
	// level and sampling decisions) and writes them to the output in order.
	WriteBatch([]BatchEntry) error
}

// WriteBatch submits a batch of entries to core.
//
// Each entry is checked independently through core's [Core.Check] pipeline,
// so level filtering, sampling, hooks, and any other per-entry logic apply
// exactly as they do for single writes. Entries admitted by the same Core are
// grouped and handed to its [BatchCore.WriteBatch] method (or written one at a
// time for Cores without a batch implementation), preserving submission
// order. Cores are processed independently: a failure on one output does not
// prevent the remaining outputs from completing.
func WriteBatch(core Core, entries []BatchEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// For each Core that admitted at least one entry, record the indices (in
	// entries) of the records it accepted. Cores are kept in the order in
	// which they first appear, and indices are appended in entry order, so
	// the fan-out and per-output ordering are stable.
	var (
		cores  []Core
		index  = make(map[Core]int)
		groups [][]int
	)
	for i := range entries {
		ent := entries[i].Entry
		if !core.Enabled(ent.Level) {
			continue
		}

		ce := core.Check(ent, nil)
		if ce == nil {
			continue
		}

		for _, c := range ce.cores {
			if j, ok := index[c]; ok {
				groups[j] = append(groups[j], i)
			} else {
				index[c] = len(cores)
				cores = append(cores, c)
				groups = append(groups, []int{i})
			}
		}
		putCheckedEntry(ce)
	}

	var err error
	for j, c := range cores {
		selected := make([]BatchEntry, len(groups[j]))
		for k, idx := range groups[j] {
			selected[k] = entries[idx]
		}

		if bc, ok := c.(BatchCore); ok {
			err = multierr.Append(err, bc.WriteBatch(selected))
			continue
		}
		for _, e := range selected {
			err = multierr.Append(err, c.Write(e.Entry, e.Fields))
		}
	}
	return err
}
