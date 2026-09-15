// Copyright (c) 2026 Uber Technologies, Inc.
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
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingSyncFile wraps a durableFile so tests can force Sync failures and
// observe Close. syncErr only takes effect after failSyncAfter successful
// Sync calls, which lets header fsyncs during initialize succeed.
type failingSyncFile struct {
	durableFile
	syncErr       error
	failSyncAfter int
	syncCalls     int
	closed        bool
}

func (f *failingSyncFile) Sync() error {
	f.syncCalls++
	if f.syncErr != nil && f.syncCalls > f.failSyncAfter {
		return f.syncErr
	}
	return f.durableFile.Sync()
}

func (f *failingSyncFile) Close() error {
	f.closed = true
	return f.durableFile.Close()
}

func verifiablePaths(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	return filepath.Join(dir, "audit.log"), filepath.Join(dir, "audit.creds")
}

func appendRawFrame(t *testing.T, path string, payload []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	defer f.Close()

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	_, err = f.Write(header[:])
	require.NoError(t, err)
	_, err = f.Write(payload)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
}

// Rubric 1: records coalesced in the buffer come back individually and
// byte-for-byte, including multiline and unterminated records.
func TestVerifiableWriteSyncer_LosslessRoundTrip(t *testing.T) {
	dataPath, credPath := verifiablePaths(t)
	ws := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))

	records := [][]byte{
		[]byte("plain record\n"),
		[]byte("multi\nline\nrecord\n"),
		[]byte("record without trailing newline"),
		[]byte(""),
		[]byte("unicode 日志 \xff\xfe binary\n"),
		[]byte("\n\nleading and trailing newlines\n\n"),
	}
	for _, r := range records {
		n, err := ws.Write(r)
		require.NoError(t, err)
		require.Equal(t, len(r), n)
	}

	// Before Sync the records live in the in-memory buffer: nothing has
	// been pushed to the data file yet.
	info, err := os.Stat(dataPath)
	require.NoError(t, err)
	require.Equal(t, int64(len(verifiableLogMagic)), info.Size(),
		"buffered records must not reach the data file before flush")

	require.NoError(t, ws.Sync())
	require.NoError(t, ws.Stop())

	got, err := VerifyVerifiableLog(dataPath, VerifiableCredentialPath(credPath))
	require.NoError(t, err)
	require.Equal(t, len(records), len(got.Records))
	for i, want := range records {
		assert.Equal(t, want, got.Records[i], "record %d was not recovered losslessly", i)
	}
	assert.Equal(t, len(records), got.Credential.Records)

	// The frame reader alone must also recover every record.
	f, err := os.Open(dataPath)
	require.NoError(t, err)
	defer f.Close()
	streamed, err := ReadVerifiableRecords(f)
	require.NoError(t, err)
	assert.Equal(t, records, streamed)
}

// Rubric 1 (concurrency): many concurrent writers producing many records
// through a tiny coalescing buffer must all come back intact when the log
// is verified.
func TestVerifiableWriteSyncer_ConcurrentCoalescedWrites(t *testing.T) {
	dataPath, credPath := verifiablePaths(t)
	ws := &VerifiableWriteSyncer{
		Path:           dataPath,
		CredentialPath: credPath,
		Size:           64, // far smaller than the records: constant merging
	}

	const goroutines, perGoroutine = 16, 50
	var wg sync.WaitGroup
	want := make(map[string]int, goroutines*perGoroutine)
	var wantMu sync.Mutex

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				msg := fmt.Sprintf("goroutine-%d\nline2-%d\nno-newline-%d", g, i, i)
				_, err := ws.Write([]byte(msg))
				assert.NoError(t, err)
				wantMu.Lock()
				want[msg]++
				wantMu.Unlock()
			}
		}(g)
	}
	wg.Wait()

	require.NoError(t, ws.Stop())

	got, err := VerifyVerifiableLog(dataPath, VerifiableCredentialPath(credPath))
	require.NoError(t, err)
	require.Len(t, got.Records, goroutines*perGoroutine)

	gotCounts := make(map[string]int, len(got.Records))
	for _, r := range got.Records {
		gotCounts[string(r)]++
	}
	assert.Equal(t, want, gotCounts, "records were lost or mutated by coalesced writes")
}

// The syncer also preserves records when driven by a real zapcore.Core,
// which submits one encoded entry per Write call.
func TestVerifiableWriteSyncer_WithCore(t *testing.T) {
	dataPath, credPath := verifiablePaths(t)
	ws := &VerifiableWriteSyncer{
		Path:           dataPath,
		CredentialPath: credPath,
		Size:           128,
	}
	core := NewCore(NewJSONEncoder(EncoderConfig{
		MessageKey: "msg",
	}), ws, DebugLevel)

	const n = 200
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				msg := fmt.Sprintf("g%d-m%d\nsecond-line", g, i)
				require.NoError(t, core.Write(Entry{Level: InfoLevel, Message: msg}, nil))
			}
		}(g)
	}
	wg.Wait()
	require.NoError(t, ws.Stop())

	got, err := VerifyVerifiableLog(dataPath, VerifiableCredentialPath(credPath))
	require.NoError(t, err)
	require.Len(t, got.Records, 8*n)
	for _, rec := range got.Records {
		assert.True(t, bytes.HasSuffix(rec, []byte("\n")), "JSON entries must keep their line ending")
		assert.Contains(t, string(rec), `"msg":"`, "entry was not recovered as its original JSON bytes")
		assert.Contains(t, string(rec), `\nsecond-line`, "embedded newline must be preserved escaped")
	}
}

// Rubric 2: modifying or reordering confirmed records, or damaging the
// independent credentials, must fail verification.
func TestVerifiableWriteSyncer_TamperDetected(t *testing.T) {
	dataPath, credPath := verifiablePaths(t)
	ws := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
	for _, msg := range []string{"alpha\n", "beta\n", "gamma\n", "delta\n"} {
		_, err := ws.Write([]byte(msg))
		require.NoError(t, err)
	}
	require.NoError(t, ws.Stop())

	good, err := VerifyVerifiableLog(dataPath, VerifiableCredentialPath(credPath))
	require.NoError(t, err)
	require.Len(t, good.Records, 4)

	t.Run("modified record", func(t *testing.T) {
		dir := t.TempDir()
		d2, c2 := copyPair(t, dataPath, credPath, dir)
		flipByte(t, d2, int64(len(verifiableLogMagic))+4+1) // inside "alpha"
		_, err := VerifyVerifiableLog(d2, VerifiableCredentialPath(c2))
		assert.ErrorIs(t, err, ErrVerifiableLogCorrupted)
	})

	t.Run("reordered records", func(t *testing.T) {
		dir := t.TempDir()
		d2, _ := copyPair(t, dataPath, credPath, dir)

		// Swap the payloads of the first two frames by rewriting the file.
		original, err := os.ReadFile(d2)
		require.NoError(t, err)
		reordered := swapFirstTwoFrames(t, original)
		require.NoError(t, os.WriteFile(d2, reordered, 0o644))

		_, err = VerifyVerifiableLog(d2, VerifiableCredentialPath(filepath.Join(dir, "audit.creds")))
		assert.ErrorIs(t, err, ErrVerifiableLogCorrupted)
	})

	t.Run("damaged credential file", func(t *testing.T) {
		dir := t.TempDir()
		d2, c2 := copyPair(t, dataPath, credPath, dir)
		flipByte(t, c2, int64(len(verifiableCredMagic))+digestSize+1)
		_, err := VerifyVerifiableLog(d2, VerifiableCredentialPath(c2))
		assert.ErrorIs(t, err, ErrVerifiableLogCorrupted)
	})
}

// Rubric 3: deleting a confirmed tail must be detected as incomplete.
func TestVerifiableWriteSyncer_TruncationDetected(t *testing.T) {
	dataPath, credPath := verifiablePaths(t)
	ws := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
	last := []byte("tail record\n")
	for _, msg := range [][]byte{[]byte("one\n"), []byte("two\n"), last} {
		_, err := ws.Write(msg)
		require.NoError(t, err)
	}
	require.NoError(t, ws.Stop())

	good, err := VerifyVerifiableLog(dataPath, VerifiableCredentialPath(credPath))
	require.NoError(t, err)

	// Remove exactly the last confirmed frame.
	shortSize := good.Credential.Size - int64(4+len(last))
	require.NoError(t, os.Truncate(dataPath, shortSize))

	_, err = VerifyVerifiableLog(dataPath, VerifiableCredentialPath(credPath))
	assert.ErrorIs(t, err, ErrVerifiableLogTruncated)
}

// Rubric 4: an interrupted writer may leave a complete unconfirmed frame
// or a torn partial frame behind. Recovery must drop only that tail and
// keep every confirmed record.
func TestVerifiableWriteSyncer_RecoveryTruncatesUnconfirmedTail(t *testing.T) {
	dataPath, credPath := verifiablePaths(t)
	ws := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
	confirmed := [][]byte{[]byte("confirmed-1\n"), []byte("confirmed-2\n")}
	for _, msg := range confirmed {
		_, err := ws.Write(msg)
		require.NoError(t, err)
	}
	require.NoError(t, ws.Sync())
	require.NoError(t, ws.Stop())

	// Simulate a crash: a fully framed but unconfirmed record followed by
	// a torn partial frame, none of it covered by credentials.
	appendRawFrame(t, dataPath, []byte("unconfirmed\n"))
	tail, err := os.OpenFile(dataPath, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = tail.Write([]byte{0x00, 0x00, 0x10}) // claims 4096 bytes, has none
	require.NoError(t, err)
	require.NoError(t, tail.Close())

	reopened := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
	more := []byte("after recovery\n")
	_, err = reopened.Write(more)
	require.NoError(t, err)
	require.NoError(t, reopened.Sync())
	require.NoError(t, reopened.Stop())

	got, err := VerifyVerifiableLog(dataPath, VerifiableCredentialPath(credPath))
	require.NoError(t, err)
	require.Equal(t, append(confirmed, more), got.Records)
	assert.NotContains(t, string(bytes.Join(got.Records, nil)), "unconfirmed")
}

// Rubric 5: damage inside the confirmed region must not be auto-repaired;
// reopening refuses to append to the file.
func TestVerifiableWriteSyncer_RecoveryRejectsCorruption(t *testing.T) {
	t.Run("confirmed byte flipped", func(t *testing.T) {
		dataPath, credPath := verifiablePaths(t)
		ws := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
		_, err := ws.Write([]byte("important\n"))
		require.NoError(t, err)
		require.NoError(t, ws.Stop())

		flipByte(t, dataPath, int64(len(verifiableLogMagic))+4)

		next := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
		_, err = next.Write([]byte("must be refused\n"))
		assert.ErrorIs(t, err, ErrVerifiableLogCorrupted)
	})

	t.Run("credential file missing", func(t *testing.T) {
		dataPath, credPath := verifiablePaths(t)
		ws := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
		_, err := ws.Write([]byte("important\n"))
		require.NoError(t, err)
		require.NoError(t, ws.Stop())

		require.NoError(t, os.Remove(credPath))
		next := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
		_, err = next.Write([]byte("must be refused\n"))
		assert.ErrorIs(t, err, ErrVerifiableLogCorrupted)
	})
}

// Rubric 6: when the underlying data or credential sync fails, Sync must
// return an error and must not advance the confirmed credential or append
// a credential covering the pending data.
func TestVerifiableWriteSyncer_FailedSyncNoCredential(t *testing.T) {
	t.Run("data sync fails", func(t *testing.T) {
		dataPath, credPath := verifiablePaths(t)
		ws := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
		injected := errors.New("disk unavailable")
		var dataFile *failingSyncFile
		ws.openDataFile = func(path string) (durableFile, error) {
			f, err := openDurableFile(path)
			if err != nil {
				return nil, err
			}
			// Succeed the header fsync (call 1), fail every later Sync.
			dataFile = &failingSyncFile{durableFile: f, syncErr: injected, failSyncAfter: 1}
			return dataFile, nil
		}

		_, err := ws.Write([]byte("pending-a\n"))
		require.NoError(t, err)
		beforeCred, beforeOK := ws.Credential()
		credInfo, err := os.Stat(credPath)
		require.NoError(t, err)
		beforeCredSize := credInfo.Size()

		err = ws.Sync()
		assert.ErrorIs(t, err, injected)

		// No credential may have been published for the pending data.
		credInfo, err = os.Stat(credPath)
		require.NoError(t, err)
		assert.Equal(t, beforeCredSize, credInfo.Size(),
			"failed sync must not append credentials")
		afterCred, afterOK := ws.Credential()
		assert.Equal(t, beforeOK, afterOK)
		assert.Equal(t, beforeCred, afterCred)

		// Once the underlying sync succeeds again, the pending records are
		// confirmed and verify cleanly.
		dataFile.syncErr = nil
		require.NoError(t, ws.Sync())
		require.NoError(t, ws.Stop())

		got, err := VerifyVerifiableLog(dataPath, VerifiableCredentialPath(credPath))
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("pending-a\n")}, got.Records)
	})

	t.Run("credential sync fails", func(t *testing.T) {
		dataPath, credPath := verifiablePaths(t)
		ws := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
		injected := errors.New("credential disk unavailable")
		ws.openCredentialFile = func(path string) (durableFile, error) {
			f, err := openDurableFile(path)
			if err != nil {
				return nil, err
			}
			// Succeed the header fsync (call 1), fail every later Sync.
			return &failingSyncFile{durableFile: f, syncErr: injected, failSyncAfter: 1}, nil
		}

		_, err := ws.Write([]byte("pending-b\n"))
		require.NoError(t, err)
		err = ws.Sync()
		assert.ErrorIs(t, err, injected)

		_, ok := ws.Credential()
		assert.False(t, ok, "credential must not advance when its sync failed")

		// The in-memory confirmation did not happen. A failed fsync gives
		// no durability guarantee, so emulate the crash in which the
		// credential bytes did not survive: close the syncer and roll the
		// credential file back to its header.
		assert.ErrorIs(t, ws.Stop(), injected)
		require.NoError(t, os.Truncate(credPath, int64(len(verifiableCredMagic))))

		// Data reached disk but was never confirmed, so reopening treats
		// it as an unconfirmed tail and drops it.
		next := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
		_, err = next.Write([]byte("fresh\n"))
		require.NoError(t, err)
		require.NoError(t, next.Stop())
		got, err := VerifyVerifiableLog(dataPath, VerifiableCredentialPath(credPath))
		require.NoError(t, err)
		assert.Equal(t, [][]byte{[]byte("fresh\n")}, got.Records)
	})
}

// Rubric 7: Stop must close the data and credential files; writes after
// Stop are rejected and stopping twice is harmless.
func TestVerifiableWriteSyncer_StopReleasesResources(t *testing.T) {
	dataPath, credPath := verifiablePaths(t)
	ws := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
	var dataFile, credFile *failingSyncFile
	ws.openDataFile = func(path string) (durableFile, error) {
		f, err := openDurableFile(path)
		require.NoError(t, err)
		dataFile = &failingSyncFile{durableFile: f}
		return dataFile, nil
	}
	ws.openCredentialFile = func(path string) (durableFile, error) {
		f, err := openDurableFile(path)
		require.NoError(t, err)
		credFile = &failingSyncFile{durableFile: f}
		return credFile, nil
	}

	_, err := ws.Write([]byte("release me\n"))
	require.NoError(t, err)
	require.NoError(t, ws.Stop())

	assert.True(t, dataFile.closed, "data file must be closed on Stop")
	assert.True(t, credFile.closed, "credential file must be closed on Stop")
	ws.mu.Lock()
	assert.Nil(t, ws.data)
	assert.Nil(t, ws.creds)
	ws.mu.Unlock()

	_, err = ws.Write([]byte("after stop\n"))
	assert.ErrorIs(t, err, ErrVerifiableSyncerStopped)
	assert.ErrorIs(t, ws.Sync(), ErrVerifiableSyncerStopped)
	assert.NoError(t, ws.Stop(), "second Stop must be a no-op")
}

// Rubric 8: without opting into verifiable output, JSON and console
// encoding bytes are exactly what they always were.
func TestVerifiableWriteSyncer_DefaultOutputUnchanged(t *testing.T) {
	ent := Entry{
		Level:   InfoLevel,
		Message: "hello\nworld",
		Stack:   "main.foo()\n\t/thing.go:1",
	}

	var jsonBuf bytes.Buffer
	jsonCore := NewCore(
		NewJSONEncoder(EncoderConfig{
			MessageKey:    "M",
			StacktraceKey: "S",
			LineEnding:    "\n",
		}),
		AddSync(&jsonBuf),
		DebugLevel,
	)
	require.NoError(t, jsonCore.Write(ent, nil))
	assert.Equal(t,
		`{"M":"hello\nworld","S":"main.foo()\n\t/thing.go:1"}`+"\n",
		jsonBuf.String())

	var consoleBuf bytes.Buffer
	consoleCore := NewCore(
		NewConsoleEncoder(EncoderConfig{
			MessageKey:    "msg",
			LevelKey:      "level",
			StacktraceKey: "stack",
			EncodeLevel:   LowercaseLevelEncoder,
			LineEnding:    "\n",
		}),
		AddSync(&consoleBuf),
		DebugLevel,
	)
	require.NoError(t, consoleCore.Write(ent, nil))
	assert.Equal(t, "info\thello\nworld\nmain.foo()\n\t/thing.go:1\n",
		consoleBuf.String())
}

func TestVerifiableWriteSyncer_CustomCredentialPath(t *testing.T) {
	dir := t.TempDir()
	dataPath := filepath.Join(dir, "audit.log")
	credPath := filepath.Join(dir, "elsewhere", "creds.bin")

	ws := NewVerifiableWriteSyncer(dataPath, VerifiableCredentialPath(credPath))
	_, err := ws.Write([]byte("custom cred location\n"))
	require.NoError(t, err)
	require.NoError(t, ws.Stop())

	_, err = os.Stat(filepath.Join(dir, "audit.log.credentials"))
	assert.True(t, os.IsNotExist(err))
	got, err := VerifyVerifiableLog(dataPath, VerifiableCredentialPath(credPath))
	require.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("custom cred location\n")}, got.Records)
}

func TestLogCredential_String(t *testing.T) {
	c := LogCredential{Records: 2, Size: 42}
	s := c.String()
	assert.Contains(t, s, "2:42:")
	assert.Len(t, s, len("2:42:")+digestSize*2)
}

// --- helpers ---

func copyPair(t *testing.T, dataPath, credPath, dir string) (string, string) {
	t.Helper()
	d2 := filepath.Join(dir, "audit.log")
	c2 := filepath.Join(dir, "audit.creds")
	copyFile(t, dataPath, d2)
	copyFile(t, credPath, c2)
	return d2, c2
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dst, b, 0o644))
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	require.NoError(t, err)
	defer f.Close()

	var b [1]byte
	_, err = f.ReadAt(b[:], off)
	require.NoError(t, err)
	if b[0] == 'a' {
		b[0] = 'z'
	} else {
		b[0] = 'a'
	}
	_, err = f.WriteAt(b[:], off)
	require.NoError(t, err)
}

func swapFirstTwoFrames(t *testing.T, raw []byte) []byte {
	t.Helper()
	off := len(verifiableLogMagic)
	readFrame := func() []byte {
		length := binary.BigEndian.Uint32(raw[off : off+4])
		start := off + 4
		end := start + int(length)
		p := append([]byte(nil), raw[start:end]...)
		off = end
		return p
	}
	first := readFrame()
	second := readFrame()
	secondEnd := off

	var out bytes.Buffer
	out.Write(raw[:len(verifiableLogMagic)])
	writeFrame := func(p []byte) {
		var h [4]byte
		binary.BigEndian.PutUint32(h[:], uint32(len(p)))
		out.Write(h[:])
		out.Write(p)
	}
	writeFrame(second)
	writeFrame(first)
	out.Write(raw[secondEnd:])
	return out.Bytes()
}
