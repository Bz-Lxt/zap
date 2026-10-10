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
	"testing"

	"go.uber.org/zap/internal/ztest"
	//revive:disable:dot-imports
	. "go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withTee(f func(core Core, debugLogs, warnLogs *observer.ObservedLogs)) {
	debugLogger, debugLogs := observer.New(DebugLevel)
	warnLogger, warnLogs := observer.New(WarnLevel)
	tee := NewTee(debugLogger, warnLogger)
	f(tee, debugLogs, warnLogs)
}

func TestTeeUnusualInput(t *testing.T) {
	// Verify that Tee handles receiving one and no inputs correctly.
	t.Run("one input", func(t *testing.T) {
		obs, _ := observer.New(DebugLevel)
		assert.Equal(t, obs, NewTee(obs), "Expected to return single inputs unchanged.")
	})
	t.Run("no input", func(t *testing.T) {
		assert.Equal(t, NewNopCore(), NewTee(), "Expected to return NopCore.")
	})
}

func TestLevelOfTee(t *testing.T) {
	debugLogger, _ := observer.New(DebugLevel)
	warnLogger, _ := observer.New(WarnLevel)

	tests := []struct {
		desc string
		give []Core
		want Level
	}{
		{desc: "empty", want: InvalidLevel},
		{
			desc: "debug",
			give: []Core{debugLogger},
			want: DebugLevel,
		},
		{
			desc: "warn",
			give: []Core{warnLogger},
			want: WarnLevel,
		},
		{
			desc: "debug and warn",
			give: []Core{warnLogger, debugLogger},
			want: DebugLevel,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()

			core := NewTee(tt.give...)
			assert.Equal(t, tt.want, LevelOf(core), "Level of Tee core did not match.")
		})
	}
}

func TestTeeCheck(t *testing.T) {
	withTee(func(tee Core, debugLogs, warnLogs *observer.ObservedLogs) {
		debugEntry := Entry{Level: DebugLevel, Message: "log-at-debug"}
		infoEntry := Entry{Level: InfoLevel, Message: "log-at-info"}
		warnEntry := Entry{Level: WarnLevel, Message: "log-at-warn"}
		errorEntry := Entry{Level: ErrorLevel, Message: "log-at-error"}
		for _, ent := range []Entry{debugEntry, infoEntry, warnEntry, errorEntry} {
			if ce := tee.Check(ent, nil); ce != nil {
				ce.Write()
			}
		}

		assert.Equal(t, []observer.LoggedEntry{
			{Entry: debugEntry, Context: []Field{}},
			{Entry: infoEntry, Context: []Field{}},
			{Entry: warnEntry, Context: []Field{}},
			{Entry: errorEntry, Context: []Field{}},
		}, debugLogs.All())

		assert.Equal(t, []observer.LoggedEntry{
			{Entry: warnEntry, Context: []Field{}},
			{Entry: errorEntry, Context: []Field{}},
		}, warnLogs.All())
	})
}

func TestTeeWrite(t *testing.T) {
	// Calling the tee's Write method directly should always log, regardless of
	// the configured level.
	withTee(func(tee Core, debugLogs, warnLogs *observer.ObservedLogs) {
		debugEntry := Entry{Level: DebugLevel, Message: "log-at-debug"}
		warnEntry := Entry{Level: WarnLevel, Message: "log-at-warn"}
		for _, ent := range []Entry{debugEntry, warnEntry} {
			assert.NoError(t, tee.Write(ent, nil))
		}

		for _, logs := range []*observer.ObservedLogs{debugLogs, warnLogs} {
			assert.Equal(t, []observer.LoggedEntry{
				{Entry: debugEntry, Context: []Field{}},
				{Entry: warnEntry, Context: []Field{}},
			}, logs.All())
		}
	})
}

func TestTeeWith(t *testing.T) {
	withTee(func(tee Core, debugLogs, warnLogs *observer.ObservedLogs) {
		f := makeInt64Field("k", 42)
		tee = tee.With([]Field{f})
		ent := Entry{Level: WarnLevel, Message: "log-at-warn"}
		if ce := tee.Check(ent, nil); ce != nil {
			ce.Write()
		}

		for _, logs := range []*observer.ObservedLogs{debugLogs, warnLogs} {
			assert.Equal(t, []observer.LoggedEntry{
				{Entry: ent, Context: []Field{f}},
			}, logs.All())
		}
	})
}

func TestTeeEnabled(t *testing.T) {
	infoLogger, _ := observer.New(InfoLevel)
	warnLogger, _ := observer.New(WarnLevel)
	tee := NewTee(infoLogger, warnLogger)
	tests := []struct {
		lvl     Level
		enabled bool
	}{
		{DebugLevel, false},
		{InfoLevel, true},
		{WarnLevel, true},
		{ErrorLevel, true},
		{DPanicLevel, true},
		{PanicLevel, true},
		{FatalLevel, true},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.enabled, tee.Enabled(tt.lvl), "Unexpected Enabled result for level %s.", tt.lvl)
	}
}

func TestTeeCheckNoBranchSelected(t *testing.T) {
	// When none of the declared branches select the entry, Check must keep
	// behaving as if the level were disabled.
	warnLogger, _ := observer.New(WarnLevel)
	errorLogger, _ := observer.New(ErrorLevel)
	tee := NewTee(warnLogger, errorLogger)

	ce := tee.Check(Entry{Level: DebugLevel, Message: "log-at-debug"}, nil)
	assert.Nil(t, ce, "Expected nil CheckedEntry when no branch selects the entry.")
}

func TestTeePostWriteResults(t *testing.T) {
	debugLogger, debugLogs := observer.New(DebugLevel)
	warnLogger, warnLogs := observer.New(WarnLevel)

	failLogger := NewCore(
		NewJSONEncoder(testEncoderConfig()),
		&ztest.FailWriter{},
		DebugLevel,
	)

	tee := NewTee(debugLogger, warnLogger, failLogger)
	ent := Entry{Level: InfoLevel, Message: "log-at-info"}

	var results []WriteResult
	ce := tee.Check(ent, nil)
	require.NotNil(t, ce)
	ce = ce.AfterWrite(ent, func(_ Entry, _ []Field, rs []WriteResult) error {
		results = rs
		return nil
	})
	ce.Write()

	require.Len(t, results, 3, "Expected one result per declared branch.")

	assert.Equal(t, debugLogger, results[0].Core)
	assert.Equal(t, WriteSucceeded, results[0].Status)
	assert.NoError(t, results[0].Err)

	assert.Equal(t, warnLogger, results[1].Core)
	assert.Equal(t, WriteNotSelected, results[1].Status)
	assert.Nil(t, results[1].Err)

	assert.Equal(t, failLogger, results[2].Core)
	assert.Equal(t, WriteFailed, results[2].Status)
	assert.EqualError(t, results[2].Err, "failed")

	// Delivery still happened to every branch that selected the entry,
	// despite the failing branch.
	assert.Equal(t, 1, debugLogs.Len())
	assert.Equal(t, 0, warnLogs.Len())
}

func TestTeeNestedPostWriteResults(t *testing.T) {
	// Nested fan-outs must report their branches in declaration order and
	// must not deliver entries twice.
	debugLogger, debugLogs := observer.New(DebugLevel)
	warnLogger, _ := observer.New(WarnLevel)
	infoLogger, infoLogs := observer.New(InfoLevel)

	inner := NewTee(debugLogger, warnLogger)
	outer := NewTee(inner, infoLogger)

	ent := Entry{Level: InfoLevel, Message: "log-at-info"}
	var results []WriteResult
	ce := outer.Check(ent, nil)
	require.NotNil(t, ce)
	ce = ce.AfterWrite(ent, func(_ Entry, _ []Field, rs []WriteResult) error {
		results = rs
		return nil
	})
	ce.Write()

	require.Len(t, results, 4)
	assert.Equal(t, inner, results[0].Core)
	assert.Equal(t, WriteSucceeded, results[0].Status)
	assert.Equal(t, debugLogger, results[1].Core)
	assert.Equal(t, WriteSucceeded, results[1].Status)
	assert.Equal(t, warnLogger, results[2].Core)
	assert.Equal(t, WriteNotSelected, results[2].Status)
	assert.Equal(t, infoLogger, results[3].Core)
	assert.Equal(t, WriteSucceeded, results[3].Status)

	assert.Equal(t, 1, debugLogs.Len(), "Branch must be written exactly once.")
	assert.Equal(t, 1, infoLogs.Len(), "Branch must be written exactly once.")
}

func TestTeeSync(t *testing.T) {
	infoLogger, _ := observer.New(InfoLevel)
	warnLogger, _ := observer.New(WarnLevel)
	tee := NewTee(infoLogger, warnLogger)
	assert.NoError(t, tee.Sync(), "Unexpected error from Syncing a tee.")

	sink := &ztest.Discarder{}
	err := errors.New("failed")
	sink.SetError(err)

	noSync := NewCore(
		NewJSONEncoder(testEncoderConfig()),
		sink,
		DebugLevel,
	)
	tee = NewTee(tee, noSync)
	assert.Equal(t, err, tee.Sync(), "Expected an error when part of tee can't Sync.")
}
