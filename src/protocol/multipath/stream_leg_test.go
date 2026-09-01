package multipath

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	smp3core "github.com/Superbias/smp3-multipath-kit-public/smp3core"
)

// fakeStreamLeg intentionally implements only the StreamLeg capability. It
// has no net.Conn methods, so this test catches accidental carrier coupling.
type fakeStreamLeg struct {
	reader io.Reader
	writer io.Writer
	close  func() error
	once   sync.Once
	closed chan struct{}
}

var _ smp3core.StreamLeg = (*fakeStreamLeg)(nil)

func (l *fakeStreamLeg) Read(p []byte) (int, error)  { return l.reader.Read(p) }
func (l *fakeStreamLeg) Write(p []byte) (int, error) { return l.writer.Write(p) }
func (l *fakeStreamLeg) Close() error {
	var err error
	l.once.Do(func() {
		close(l.closed)
		if l.close != nil {
			err = l.close()
		}
	})
	return err
}

func newFakeStreamLegPair() (*fakeStreamLeg, *fakeStreamLeg) {
	aToBReader, aToBWriter := io.Pipe()
	bToAReader, bToAWriter := io.Pipe()
	a := &fakeStreamLeg{
		reader: bToAReader,
		writer: aToBWriter,
		closed: make(chan struct{}),
	}
	b := &fakeStreamLeg{
		reader: aToBReader,
		writer: bToAWriter,
		closed: make(chan struct{}),
	}
	a.close = func() error {
		_ = aToBWriter.Close()
		return bToAReader.Close()
	}
	b.close = func() error {
		_ = bToAWriter.Close()
		return aToBReader.Close()
	}
	return a, b
}

func TestStreamLegWithoutNetConnRoundTrip(t *testing.T) {
	left, right := newFakeStreamLegPair()
	leftCore, leftApp := newCore(testCoreConfig())
	rightCore, rightApp := newCore(testCoreConfig())
	defer leftCore.Close()
	defer rightCore.Close()
	defer leftApp.Close()
	defer rightApp.Close()

	if err := leftCore.addLeg(0, left, nil); err != nil {
		t.Fatal(err)
	}
	if err := rightCore.addLeg(0, right, nil); err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("stream-leg"), 200)
	got := make([]byte, len(payload))
	readDone := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(rightApp, got)
		readDone <- err
	}()
	if _, err := leftApp.Write(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fake StreamLeg round-trip timed out")
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("fake StreamLeg payload mismatch")
	}
}

func TestStreamLegClosePropagates(t *testing.T) {
	leg, peer := newFakeStreamLegPair()
	core, app := newCore(testCoreConfig())
	defer app.Close()
	if err := core.addLeg(0, leg, nil); err != nil {
		t.Fatal(err)
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-leg.closed:
	case <-time.After(time.Second):
		t.Fatal("core close did not propagate to StreamLeg")
	}
	_ = peer.Close()
}

type readErrorStreamLeg struct {
	err    error
	closed chan struct{}
}

var _ smp3core.StreamLeg = (*readErrorStreamLeg)(nil)

func (l *readErrorStreamLeg) Read([]byte) (int, error)  { return 0, l.err }
func (l *readErrorStreamLeg) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
func (l *readErrorStreamLeg) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func TestStreamLegReadFailureEntersExistingFailurePath(t *testing.T) {
	wantErr := errors.New("fake read failure")
	leg := &readErrorStreamLeg{err: wantErr, closed: make(chan struct{})}
	core, app := newCore(testCoreConfig())
	defer core.Close()
	defer app.Close()
	failed := make(chan error, 1)
	if err := core.addLeg(0, leg, func(err error) { failed <- err }); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-failed:
		if !errors.Is(err, wantErr) {
			t.Fatalf("failure callback error=%v, want %v", err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("read failure did not enter existing leg failure path")
	}
	deadline := time.Now().Add(time.Second)
	for core.hasLeg(0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if core.hasLeg(0) {
		t.Fatal("read failure left failed StreamLeg attached")
	}
}

type writeErrorStreamLeg struct {
	err    error
	closed chan struct{}
}

var _ smp3core.StreamLeg = (*writeErrorStreamLeg)(nil)

func (l *writeErrorStreamLeg) Read([]byte) (int, error) {
	<-l.closed
	return 0, io.ErrClosedPipe
}
func (l *writeErrorStreamLeg) Write([]byte) (int, error) { return 0, l.err }
func (l *writeErrorStreamLeg) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func TestStreamLegWriteFailureEntersExistingFailurePath(t *testing.T) {
	wantErr := errors.New("fake write failure")
	leg := &writeErrorStreamLeg{err: wantErr, closed: make(chan struct{})}
	core, app := newCore(testCoreConfig())
	defer core.Close()
	defer app.Close()
	failed := make(chan error, 1)
	if err := core.addLeg(0, leg, func(err error) { failed <- err }); err != nil {
		t.Fatal(err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := app.Write([]byte("write-failure"))
		writeDone <- err
	}()
	select {
	case err := <-failed:
		if !errors.Is(err, wantErr) {
			t.Fatalf("failure callback error=%v, want %v", err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("write failure did not enter existing leg failure path")
	}
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("application write did not unblock after StreamLeg failure")
	}
	deadline := time.Now().Add(time.Second)
	for core.hasLeg(0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if core.hasLeg(0) {
		t.Fatal("write failure left failed StreamLeg attached")
	}
}
