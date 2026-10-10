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

package zap

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"

	"go.uber.org/zap/internal/ztest"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// batchRecordingSyncer records every Write call; it is intentionally left
// unlocked so the test relies on the batch path's own serialization.
type batchRecordingSyncer struct {
	mu     sync.Mutex
	writes [][]byte
}

func (s *batchRecordingSyncer) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.writes = append(s.writes, append([]byte(nil), p...))
	s.mu.Unlock()
	return len(p), nil
}

func (*batchRecordingSyncer) Sync() error { return nil }

func (s *batchRecordingSyncer) Writes() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

func batchJSONCore(enab zapcore.LevelEnabler, ws zapcore.WriteSyncer) zapcore.Core {
	cfg := zapcore.EncoderConfig{
		MessageKey:     "msg",
		LevelKey:       "level",
		CallerKey:      "caller",
		StacktraceKey:  "stacktrace",
		LineEnding:     "\n",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeCaller:   zapcore.FullCallerEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
	}
	return zapcore.NewCore(zapcore.NewJSONEncoder(cfg), ws, enab)
}

func TestLoggerLogBatch(t *testing.T) {
	withLogger(t, DebugLevel, opts(AddCaller()), func(log *Logger, logs *observer.ObservedLogs) {
		log.Named("test").LogBatch(
			BatchEntry{Level: zapcore.DebugLevel, Message: "one", Fields: []Field{Int("i", 1)}},
			BatchEntry{Level: zapcore.InfoLevel, Message: "two"},
			BatchEntry{Level: zapcore.ErrorLevel, Message: "three"},
		)

		all := logs.All()
		require.Len(t, all, 3)
		assert.Equal(t, "one", all[0].Message)
		assert.Equal(t, zapcore.DebugLevel, all[0].Level)
		assert.Equal(t, "test", all[0].LoggerName)
		assert.True(t, all[0].Caller.Defined)
		require.Len(t, all[0].Context, 1)
		assert.Equal(t, "i", all[0].Context[0].Key)

		assert.Equal(t, "two", all[1].Message)
		assert.Equal(t, zapcore.InfoLevel, all[1].Level)
		assert.Equal(t, "three", all[2].Message)
		assert.Equal(t, zapcore.ErrorLevel, all[2].Level)
	})
}

func TestLoggerLogBatch_LevelFiltering(t *testing.T) {
	withLogger(t, InfoLevel, nil, func(log *Logger, logs *observer.ObservedLogs) {
		log.LogBatch(
			BatchEntry{Level: zapcore.DebugLevel, Message: "debug"},
			BatchEntry{Level: zapcore.InfoLevel, Message: "info"},
		)
		all := logs.All()
		require.Len(t, all, 1)
		assert.Equal(t, "info", all[0].Message)
	})
}

func TestLoggerLogBatch_TerminalLevelsIgnored(t *testing.T) {
	withLogger(t, DebugLevel, nil, func(log *Logger, logs *observer.ObservedLogs) {
		log.LogBatch(
			BatchEntry{Level: zapcore.DPanicLevel, Message: "dpanic"},
			BatchEntry{Level: zapcore.PanicLevel, Message: "panic"},
			BatchEntry{Level: zapcore.FatalLevel, Message: "fatal"},
		)
		assert.Empty(t, logs.All())
	})
}

func TestLoggerLogBatch_Empty(t *testing.T) {
	withLogger(t, DebugLevel, nil, func(log *Logger, logs *observer.ObservedLogs) {
		log.LogBatch()
		assert.Empty(t, logs.All())
	})
}

func TestLoggerLogBatch_SingleWriteCompatible(t *testing.T) {
	withLogger(t, DebugLevel, nil, func(log *Logger, logs *observer.ObservedLogs) {
		log.Info("before")
		log.LogBatch(BatchEntry{Level: zapcore.InfoLevel, Message: "batched"})
		log.Info("after")

		all := logs.All()
		require.Len(t, all, 3)
		assert.Equal(t, "before", all[0].Message)
		assert.Equal(t, "batched", all[1].Message)
		assert.Equal(t, "after", all[2].Message)
	})
}

func TestLoggerLogBatch_Caller(t *testing.T) {
	buf := &ztest.Buffer{}
	log := New(batchJSONCore(DebugLevel, buf), AddCaller())

	_, file, line, _ := runtime.Caller(0) // LogBatch must be the next line
	log.LogBatch(BatchEntry{Level: zapcore.InfoLevel, Message: "called"})

	require.Len(t, buf.Lines(), 1)
	want := fmt.Sprintf("%s:%d", file, line+1)
	assert.Contains(t, buf.Lines()[0], fmt.Sprintf(`"caller":"%s"`, want))
}

func TestLoggerLogBatch_Stacktrace(t *testing.T) {
	buf := &ztest.Buffer{}
	log := New(batchJSONCore(DebugLevel, buf), AddStacktrace(zapcore.WarnLevel))

	log.LogBatch(
		BatchEntry{Level: zapcore.InfoLevel, Message: "no-trace"},
		BatchEntry{Level: zapcore.WarnLevel, Message: "with-trace"},
	)

	lines := buf.Lines()
	require.Len(t, lines, 2)
	assert.NotContains(t, lines[0], `"stacktrace"`)
	assert.Contains(t, lines[1], `"stacktrace"`)
	assert.Contains(t, lines[1], "logger_batch_test.go")
}

func TestLoggerLogBatch_ConcurrentBatchesDoNotInterleave(t *testing.T) {
	syncer := &batchRecordingSyncer{}
	log := New(batchJSONCore(DebugLevel, syncer))

	const (
		goroutines = 8
		batches    = 20
		batchSize  = 5
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				entries := make([]BatchEntry, batchSize)
				for i := 0; i < batchSize; i++ {
					entries[i] = BatchEntry{
						Level:   zapcore.InfoLevel,
						Message: fmt.Sprintf("g%d-b%d-i%d", g, b, i),
					}
				}
				log.LogBatch(entries...)
			}
		}(g)
	}
	wg.Wait()

	writes := syncer.Writes()
	require.Len(t, writes, goroutines*batches)
	for _, payload := range writes {
		lines := strings.Split(strings.TrimRight(string(payload), "\n"), "\n")
		require.Len(t, lines, batchSize, "batch payload must stay contiguous")

		key := func(line string) string {
			msg := line[strings.Index(line, `"msg":"`)+7:]
			msg = msg[:strings.Index(msg, `"`)]
			return msg[:strings.LastIndex(msg, "-")]
		}
		want := key(lines[0])
		for _, line := range lines {
			assert.Equal(t, want, key(line), "records from different batches must not interleave")
		}
	}
}

// A failing output reports an error to the logger's error output but must not
// prevent the rest of the batch pipeline from completing.
func TestLoggerLogBatch_WriteErrorReported(t *testing.T) {
	errOut := &ztest.Buffer{}
	core := batchJSONCore(DebugLevel, &failingBatchSyncer{err: errors.New("nope")})
	log := New(core, ErrorOutput(errOut))

	log.LogBatch(BatchEntry{Level: zapcore.InfoLevel, Message: "x"})
	assert.Contains(t, errOut.String(), "write error")
}

type failingBatchSyncer struct {
	err error
}

func (s *failingBatchSyncer) Write([]byte) (int, error) { return 0, s.err }
func (s *failingBatchSyncer) Sync() error               { return nil }
