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
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/multierr"
)

// Verifiable output lets callers persist local audit logs together with
// independently stored verification credentials. The credentials form a
// hash chain over the exact bytes of every confirmed record, so an
// auditor can later detect:
//
//   - modification of any confirmed record,
//   - reordering of confirmed records,
//   - deletion of a confirmed tail, and
//   - torn writes left behind by an interrupted process.
//
// Records are length-prefixed on disk, so buffering and concurrent writes
// may coalesce multiple records into a single underlying write without
// losing record boundaries. Records that contain embedded newlines or no
// trailing newline are recovered byte-for-byte.
//
// A record is only confirmed once its bytes have been flushed and synced
// to the data file, and the covering credential has subsequently been
// synced to the credential file. If the process is interrupted before
// confirmation, the surplus bytes are treated as an unconfirmed tail: on
// the next open they are discarded, and appending resumes at the last
// confirmation point. Corruption inside the confirmed region is never
// repaired automatically; the syncer refuses to append to such a file.
//
// Because credentials are stored separately, tamper detection is only
// meaningful if the credential file is kept on storage that the log
// writer's adversary cannot also rewrite. If an attacker can modify both
// files, the chain can be forged.
const (
	// verifiableLogMagic prefixes every verifiable data file.
	verifiableLogMagic = "ZAPVLOG1\n"

	// verifiableCredMagic prefixes every credential file.
	verifiableCredMagic = "ZAPVCRED1\n"

	// credentialSuffix is appended to the data file path when no explicit
	// credential file path is provided.
	credentialSuffix = ".credentials"

	// digestSize is the size of a single chained credential.
	digestSize = sha256.Size

	// maxVerifiableRecordSize guards against absurdly large length
	// prefixes in (possibly damaged) data files.
	maxVerifiableRecordSize = 512 << 20 // 512 MiB
)

var (
	// ErrVerifiableLogCorrupted is returned when a confirmed region fails
	// verification: a record's bytes do not match its credential, the
	// records are in the wrong order, the credential file itself is
	// damaged, or data exists for which there is no credential.
	ErrVerifiableLogCorrupted = errors.New("zapcore: verifiable log is corrupted")

	// ErrVerifiableLogTruncated is returned when confirmed records are
	// missing: the data file ends before all credentials are covered.
	ErrVerifiableLogTruncated = errors.New("zapcore: verifiable log is truncated")

	// ErrVerifiableSyncerStopped is returned when Write or Sync is called
	// after the syncer has been stopped.
	ErrVerifiableSyncerStopped = errors.New("zapcore: verifiable write syncer stopped")
)

// LogCredential is an independently persisted verification credential. It
// covers the first Records records, exactly Size bytes of the data file,
// and its Digest is the head of the hash chain after those records.
type LogCredential struct {
	// Records is the number of confirmed records covered by this
	// credential.
	Records int

	// Size is the size in bytes of the confirmed region of the data
	// file, including its header.
	Size int64

	// Digest is the chained digest of every confirmed record.
	Digest [digestSize]byte
}

// String renders the credential as "records:size:hex-digest".
func (c LogCredential) String() string {
	return fmt.Sprintf("%d:%d:%s", c.Records, c.Size, hex.EncodeToString(c.Digest[:]))
}

// verifiableOptions holds the configuration shared by the writing and
// reading entry points.
type verifiableOptions struct {
	credentialPath string
}

// VerifiableOption customizes verifiable log readers and writers.
type VerifiableOption func(*verifiableOptions)

// VerifiableCredentialPath selects the path of the credential file, which
// is stored separately from the log data. Defaults to the data file path
// with ".credentials" appended.
func VerifiableCredentialPath(path string) VerifiableOption {
	return func(o *verifiableOptions) {
		o.credentialPath = path
	}
}

// durableFile is the subset of *os.File operations required by the
// verifiable syncer.
type durableFile interface {
	io.Writer
	io.ReaderAt
	Sync() error
	Truncate(size int64) error
	Stat() (os.FileInfo, error)
	io.Closer
}

// VerifiableWriteSyncer is a WriteSyncer for local audit logs that frames
// every Write as one losslessly recoverable record, buffers and coalesces
// frames like BufferedWriteSyncer, and durably publishes independent
// verification credentials on Sync.
//
// Each call to Write is treated as exactly one log record. This matches
// zapcore.ioCore, which submits one encoded entry per Write call; a
// record therefore keeps its boundary even though the background buffer
// merges many writes.
//
// VerifiableWriteSyncer is safe for concurrent use and does not need to
// be wrapped with Lock.
//
// Always stop the syncer when it is no longer needed to flush pending
// records and release the data and credential files:
//
//	ws := zapcore.NewVerifiableWriteSyncer("audit.log")
//	defer ws.Stop()
//	core := zapcore.NewCore(enc, ws, zapcore.DebugLevel)
type VerifiableWriteSyncer struct {
	// Path is the location of the framed log data.
	//
	// This field is required.
	Path string

	// CredentialPath is the location of the independently stored
	// credentials. Defaults to Path + ".credentials".
	CredentialPath string

	// Size specifies the maximum amount of data buffered in memory
	// before buffered frames are pushed to the data file. Note that
	// data is only confirmed after Sync publishes its credentials.
	//
	// Defaults to 256 kB if unspecified.
	Size int

	// FlushInterval specifies how often buffered data is flushed and
	// confirmed.
	//
	// Defaults to 30 seconds if unspecified.
	FlushInterval time.Duration

	// Clock, if specified, provides control of the source of time.
	//
	// Defaults to the system clock.
	Clock Clock

	// openDataFile and openCredentialFile, if non-nil, replace the
	// default file openers. They are unexported seams used by tests to
	// inject failures.
	openDataFile       func(path string) (durableFile, error)
	openCredentialFile func(path string) (durableFile, error)

	mu          sync.Mutex
	initialized bool
	stopped     bool
	closed      bool
	data        durableFile
	creds       durableFile
	writer      *bufio.Writer
	ticker      *time.Ticker
	stop        chan struct{} // closed when flushLoop should stop
	done        chan struct{} // closed when flushLoop has stopped

	chain        [digestSize]byte // head digest of confirmed + pending records
	pending      [][digestSize]byte
	pendingBytes int64 // framed bytes accepted but not yet confirmed
	confirmed    LogCredential
}

// NewVerifiableWriteSyncer creates a VerifiableWriteSyncer that appends to
// the data file at path. The file (and its credential file) are opened
// lazily on the first Write; opening an existing log verifies the
// confirmed region and truncates any unconfirmed tail before appending
// resumes.
func NewVerifiableWriteSyncer(path string, opts ...VerifiableOption) *VerifiableWriteSyncer {
	cfg := verifiableOptions{credentialPath: path + credentialSuffix}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &VerifiableWriteSyncer{
		Path:           path,
		CredentialPath: cfg.credentialPath,
	}
}

// Write frames one log record and buffers it. It returns the record
// length on success, so it satisfies io.Writer.
func (s *VerifiableWriteSyncer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initialized {
		if err := s.initialize(); err != nil {
			return 0, err
		}
	}
	if s.stopped {
		return 0, ErrVerifiableSyncerStopped
	}

	if int64(len(p)) > math.MaxUint32 {
		return 0, fmt.Errorf("zapcore: verifiable record is %d bytes, frame limit is %d",
			len(p), int64(math.MaxUint32))
	}

	prevChain := s.chain
	digest := chainDigest(s.chain, p)

	// A single framed write keeps header and payload together inside the
	// bufio.Writer, which is what preserves the record boundary while
	// writes are coalesced.
	frame := make([]byte, 4+len(p))
	binary.BigEndian.PutUint32(frame, uint32(len(p)))
	copy(frame[4:], p)

	// Flush the existing buffer if this frame does not fit. bufio passes
	// oversized writes straight through when the buffer is empty.
	if len(frame) > s.writer.Available() && s.writer.Buffered() > 0 {
		if err := s.writer.Flush(); err != nil {
			return 0, err
		}
	}

	if _, err := s.writer.Write(frame); err != nil {
		// The record never entered the buffer; roll back the pending
		// chain so retries are not chained against a phantom record.
		s.chain = prevChain
		return 0, err
	}

	s.chain = digest
	s.pending = append(s.pending, digest)
	s.pendingBytes += int64(len(frame))
	return len(p), nil
}

// Sync flushes buffered records and, only after both the data and the
// credentials have been durably synced, advances the confirmed
// credential. A failed Sync never produces a successful credential for
// the records it attempted to confirm.
func (s *VerifiableWriteSyncer) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrVerifiableSyncerStopped
	}
	if !s.initialized {
		// Match BufferedWriteSyncer: Sync before the first Write is a
		// no-op.
		return nil
	}
	return s.commitLocked()
}

// Credential returns the latest durably confirmed credential. It only
// covers records whose data and credentials have both been synced; ok is
// false before the first successful Sync.
func (s *VerifiableWriteSyncer) Credential() (c LogCredential, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.confirmed, s.initialized && s.confirmed.Size > int64(len(verifiableLogMagic))
}

// flushLoop commits buffered records at the configured interval until
// Stop is called.
func (s *VerifiableWriteSyncer) flushLoop() {
	defer close(s.done)

	for {
		select {
		case <-s.ticker.C:
			// As with BufferedWriteSyncer, flush errors are surfaced by
			// the Sync call made during Stop.
			_ = s.Sync()
		case <-s.stop:
			return
		}
	}
}

func (s *VerifiableWriteSyncer) initialize() error {
	size := s.Size
	if size == 0 {
		size = _defaultBufferSize
	}

	flushInterval := s.FlushInterval
	if flushInterval == 0 {
		flushInterval = _defaultFlushInterval
	}

	if s.Clock == nil {
		s.Clock = DefaultClock
	}

	opener := s.openDataFile
	if opener == nil {
		opener = openDurableFile
	}
	openCred := s.openCredentialFile
	if openCred == nil {
		openCred = openDurableFile
	}

	for _, path := range []string{s.Path, s.CredentialPath} {
		if dir := filepath.Dir(path); dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return err
			}
		}
	}

	data, dataExisted, err := openCreate(opener, s.Path)
	if err != nil {
		return err
	}
	creds, credExisted, err := openCreate(openCred, s.CredentialPath)
	if err != nil {
		_ = data.Close()
		return err
	}

	if dataExisted != credExisted {
		_ = data.Close()
		_ = creds.Close()
		return fmt.Errorf("%w: exactly one of data file %q and credential file %q exists",
			ErrVerifiableLogCorrupted, s.Path, s.CredentialPath)
	}

	if !dataExisted {
		if err := initNewFiles(data, creds); err != nil {
			_ = data.Close()
			_ = creds.Close()
			return err
		}
	}

	confirmed, chain, err := recoverVerifiableLog(data, creds)
	if err != nil {
		_ = data.Close()
		_ = creds.Close()
		return err
	}

	s.data = data
	s.creds = creds
	s.writer = bufio.NewWriterSize(data, size)
	s.chain = chain
	s.confirmed = confirmed
	s.pending = nil
	s.pendingBytes = 0

	s.ticker = s.Clock.NewTicker(flushInterval)
	s.stop = make(chan struct{})
	s.done = make(chan struct{})
	s.initialized = true
	go s.flushLoop()
	return nil
}

// commitLocked flushes and fsyncs the data, then appends and fsyncs the
// covering credentials. The caller must hold s.mu.
func (s *VerifiableWriteSyncer) commitLocked() error {
	if s.writer.Buffered() > 0 {
		if err := s.writer.Flush(); err != nil {
			return err
		}
	}

	// Data must be durable before its credential can claim it.
	if err := s.data.Sync(); err != nil {
		return err
	}

	if len(s.pending) > 0 {
		block := make([]byte, 0, digestSize*len(s.pending))
		for _, d := range s.pending {
			block = append(block, d[:]...)
		}
		if _, err := writeAll(s.creds, block); err != nil {
			return err
		}
		if err := s.creds.Sync(); err != nil {
			return err
		}
	}

	s.confirmed.Records += len(s.pending)
	s.confirmed.Size += s.pendingBytes
	s.confirmed.Digest = s.chain
	s.pending = s.pending[:0]
	s.pendingBytes = 0
	return nil
}

// Stop stops the background goroutine, makes a final confirmation
// attempt, and releases the data and credential files. It is safe to call
// more than once.
func (s *VerifiableWriteSyncer) Stop() error {
	stopped := func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()

		if !s.initialized || s.stopped {
			return false
		}
		s.stopped = true
		s.ticker.Stop()
		close(s.stop)
		return true
	}()

	// Never initialized, or already stopped: no cleanup needed.
	if !stopped {
		return nil
	}

	// Wait for the flush loop outside of the lock, as it may need the
	// lock to finish.
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()

	commitErr := s.commitLocked()

	var closeErr error
	if err := s.data.Close(); err != nil {
		closeErr = multierr.Append(closeErr, err)
	}
	if err := s.creds.Close(); err != nil {
		closeErr = multierr.Append(closeErr, err)
	}
	s.data = nil
	s.creds = nil
	s.closed = true

	return multierr.Append(commitErr, closeErr)
}

// VerifiedLog is the result of verifying a log against its independently
// stored credentials.
type VerifiedLog struct {
	// Credential is the credential covering every record in Records.
	Credential LogCredential

	// Records are the exact original bytes of every confirmed record, in
	// order.
	Records [][]byte
}

// VerifyVerifiableLog opens a verifiable data file and verifies every
// record against the independently stored credentials. On success it
// returns the confirmed records byte-for-byte, in order. It fails with
// ErrVerifiableLogCorrupted when content, order or credentials are
// damaged, and with ErrVerifiableLogTruncated when confirmed records are
// missing from the data file.
func VerifyVerifiableLog(path string, opts ...VerifiableOption) (*VerifiedLog, error) {
	cfg := verifiableOptions{credentialPath: path + credentialSuffix}
	for _, opt := range opts {
		opt(&cfg)
	}

	data, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer data.Close()

	creds, err := os.Open(cfg.credentialPath)
	if err != nil {
		return nil, err
	}
	defer creds.Close()

	info, err := data.Stat()
	if err != nil {
		return nil, err
	}

	credInfo, err := creds.Stat()
	if err != nil {
		return nil, err
	}

	digests, err := readCredentials(creds, credInfo.Size())
	if err != nil {
		return nil, err
	}

	records, err := readFrames(data, info.Size())
	if err != nil {
		return nil, err
	}

	if len(records) < len(digests) {
		return nil, fmt.Errorf("%w: %d credentials cover only %d records",
			ErrVerifiableLogTruncated, len(digests), len(records))
	}
	if len(records) > len(digests) {
		return nil, fmt.Errorf("%w: %d records but only %d credentials",
			ErrVerifiableLogCorrupted, len(records), len(digests))
	}

	chain := emptyDigest()
	size := int64(len(verifiableLogMagic))
	for i, record := range records {
		chain = chainDigest(chain, record)
		if chain != digests[i] {
			return nil, fmt.Errorf("%w: record %d does not match its credential",
				ErrVerifiableLogCorrupted, i)
		}
		size += 4 + int64(len(record))
	}

	return &VerifiedLog{
		Credential: LogCredential{
			Records: len(records),
			Size:    size,
			Digest:  chain,
		},
		Records: records,
	}, nil
}

// ReadVerifiableRecords reads framed records from r, which must begin with
// the verifiable log header. It returns the exact original bytes of every
// record in order, including records that span multiple lines or lack a
// trailing newline. It performs no credential verification; use
// VerifyVerifiableLog for authenticated reads.
func ReadVerifiableRecords(r io.Reader) ([][]byte, error) {
	br := bufio.NewReader(r)
	if err := readMagic(br, verifiableLogMagic); err != nil {
		return nil, err
	}
	var records [][]byte
	for {
		record, eof, err := nextFrame(br, maxVerifiableRecordSize)
		if err != nil {
			return nil, err
		}
		if eof {
			return records, nil
		}
		records = append(records, record)
	}
}

// recoverVerifiableLog verifies the confirmed region of an opened pair of
// files, truncates any unconfirmed tail, and returns the confirmed
// credential together with the chain head to continue from.
func recoverVerifiableLog(data, creds durableFile) (LogCredential, [digestSize]byte, error) {
	credInfo, err := creds.Stat()
	if err != nil {
		return LogCredential{}, emptyDigest(), err
	}

	digests, err := readCredentials(creds, credInfo.Size())
	if err != nil {
		return LogCredential{}, emptyDigest(), err
	}

	info, err := data.Stat()
	if err != nil {
		return LogCredential{}, emptyDigest(), err
	}

	// Only parse the frames covered by credentials. Bytes beyond the
	// confirmed region are never inspected: a torn write may leave any
	// kind of partial garbage there, and it will simply be truncated.
	records, confirmedSize, err := readConfirmedFrames(data, info.Size(), len(digests))
	if err != nil {
		return LogCredential{}, emptyDigest(), err
	}

	chain := emptyDigest()
	for i := 0; i < len(digests); i++ {
		chain = chainDigest(chain, records[i])
		if chain != digests[i] {
			return LogCredential{}, emptyDigest(), fmt.Errorf(
				"%w: confirmed record %d does not match its credential",
				ErrVerifiableLogCorrupted, i)
		}
	}

	// Anything beyond the confirmed region is an unconfirmed tail left by
	// an interrupted writer; drop it and resume appending at the
	// confirmation point. Corruption inside the confirmed region was
	// rejected above rather than repaired.
	if info.Size() > confirmedSize {
		if err := data.Truncate(confirmedSize); err != nil {
			return LogCredential{}, emptyDigest(), fmt.Errorf(
				"zapcore: failed to truncate unconfirmed tail: %w", err)
		}
		if err := data.Sync(); err != nil {
			return LogCredential{}, emptyDigest(), fmt.Errorf(
				"zapcore: failed to sync tail truncation: %w", err)
		}
	}

	return LogCredential{
		Records: len(digests),
		Size:    confirmedSize,
		Digest:  chain,
	}, chain, nil
}

// readCredentials reads and validates the credential header and all
// chained digests.
func readCredentials(f io.ReaderAt, size int64) ([][digestSize]byte, error) {
	br := bufio.NewReader(io.NewSectionReader(f, 0, size))
	if err := readMagic(br, verifiableCredMagic); err != nil {
		return nil, err
	}

	rest, err := io.ReadAll(br)
	if err != nil {
		return nil, err
	}
	if len(rest)%digestSize != 0 {
		return nil, fmt.Errorf("%w: credential file has %d trailing bytes",
			ErrVerifiableLogCorrupted, len(rest)%digestSize)
	}

	digests := make([][digestSize]byte, 0, len(rest)/digestSize)
	for len(rest) > 0 {
		var d [digestSize]byte
		copy(d[:], rest[:digestSize])
		digests = append(digests, d)
		rest = rest[digestSize:]
	}
	return digests, nil
}

// readConfirmedFrames reads exactly want frames from the start of f and
// returns them together with the number of bytes they occupy. Failure to
// fully read any wanted frame means the confirmed region is incomplete.
func readConfirmedFrames(f io.ReaderAt, size int64, want int) ([][]byte, int64, error) {
	r := io.NewSectionReader(f, 0, size)
	br := bufio.NewReader(r)
	if err := readMagic(br, verifiableLogMagic); err != nil {
		return nil, 0, err
	}

	records := make([][]byte, 0, want)
	off := int64(len(verifiableLogMagic))
	for i := 0; i < want; i++ {
		record, n, err := nextSizedFrame(br, size-off)
		if err != nil {
			return nil, off, fmt.Errorf(
				"%w: confirmed record %d is incomplete: %v",
				ErrVerifiableLogTruncated, i, err)
		}
		records = append(records, record)
		off += int64(n)
	}
	return records, off, nil
}

// readFrames reads the data header and all complete frames from the first
// size bytes of f.
func readFrames(f io.ReaderAt, size int64) ([][]byte, error) {
	r := io.NewSectionReader(f, 0, size)
	br := bufio.NewReader(r)
	if err := readMagic(br, verifiableLogMagic); err != nil {
		return nil, err
	}

	var records [][]byte
	remaining := size - int64(len(verifiableLogMagic))
	for remaining > 0 {
		record, n, err := nextSizedFrame(br, remaining)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
		remaining -= int64(n)
	}
	return records, nil
}

// nextSizedFrame reads one length-prefixed record. It rejects prefixes
// that claim more than the bytes that remain, which catches both torn
// writes and damaged length prefixes without allocating giant buffers.
func nextSizedFrame(r io.Reader, remaining int64) ([]byte, int, error) {
	var header [4]byte
	n, err := io.ReadFull(r, header[:])
	if err != nil {
		return nil, n, fmt.Errorf("%w: incomplete frame header", ErrVerifiableLogCorrupted)
	}

	length := int64(binary.BigEndian.Uint32(header[:]))
	if length > remaining-4 || length > maxVerifiableRecordSize {
		return nil, 4, fmt.Errorf("%w: frame claims %d bytes, %d remain",
			ErrVerifiableLogCorrupted, length, remaining-4)
	}

	record := make([]byte, length)
	if _, err := io.ReadFull(r, record); err != nil {
		return nil, 4, fmt.Errorf("%w: incomplete record: %v",
			ErrVerifiableLogCorrupted, err)
	}
	return record, 4 + int(length), nil
}

// nextFrame reads one length-prefixed record from a stream bounded only by
// maxSize. It reports cleanEOF at an exact frame boundary.
func nextFrame(r *bufio.Reader, maxSize int64) (record []byte, cleanEOF bool, err error) {
	var header [4]byte
	n, err := io.ReadFull(r, header[:])
	if err == io.EOF && n == 0 {
		return nil, true, nil
	}
	if err == io.ErrUnexpectedEOF {
		return nil, false, fmt.Errorf("%w: incomplete frame header", ErrVerifiableLogCorrupted)
	}
	if err != nil {
		return nil, false, err
	}

	length := int64(binary.BigEndian.Uint32(header[:]))
	if length > maxSize {
		return nil, false, fmt.Errorf("%w: frame claims %d bytes", ErrVerifiableLogCorrupted, length)
	}

	record = make([]byte, length)
	if _, err := io.ReadFull(r, record); err != nil {
		return nil, false, fmt.Errorf("%w: incomplete record: %v",
			ErrVerifiableLogCorrupted, err)
	}
	return record, false, nil
}

func readMagic(r *bufio.Reader, magic string) error {
	buf := make([]byte, len(magic))
	if _, err := io.ReadFull(r, buf); err != nil {
		return fmt.Errorf("%w: missing header: %v", ErrVerifiableLogCorrupted, err)
	}
	if string(buf) != magic {
		return fmt.Errorf("%w: bad header %q, want %q",
			ErrVerifiableLogCorrupted, buf, magic)
	}
	return nil
}

func chainDigest(prev [digestSize]byte, record []byte) [digestSize]byte {
	h := sha256.New()
	h.Write(prev[:])
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(record)))
	h.Write(header[:])
	h.Write(record)
	var out [digestSize]byte
	copy(out[:], h.Sum(out[:0]))
	return out
}

func emptyDigest() [digestSize]byte {
	return [digestSize]byte{}
}

func writeAll(w io.Writer, p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		m, err := w.Write(p)
		n += m
		if err != nil {
			return n, err
		}
		p = p[m:]
	}
	return n, nil
}

// openCreate opens path for reading and appending, reporting whether the
// file already held data. The opener is expected to create the file if it
// does not exist.
func openCreate(open func(string) (durableFile, error), path string) (durableFile, bool, error) {
	f, err := open(path)
	if err != nil {
		return nil, false, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, false, err
	}
	return f, info.Size() > 0, nil
}

func openDurableFile(path string) (durableFile, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
}

// initNewFiles writes and fsyncs the headers of a freshly created log
// pair.
func initNewFiles(data, creds durableFile) error {
	if _, err := writeAll(data, []byte(verifiableLogMagic)); err != nil {
		return err
	}
	if err := data.Sync(); err != nil {
		return err
	}
	if _, err := writeAll(creds, []byte(verifiableCredMagic)); err != nil {
		return err
	}
	if err := creds.Sync(); err != nil {
		return err
	}
	return nil
}
