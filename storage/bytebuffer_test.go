package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

func TestByteBufferAcquireReleaseSerial(t *testing.T) {
	testByteBufferAcquireRelease(t)
}

func TestByteBuffer_Reset(t *testing.T) {
	b := AcquireByteBuffer()
	if _, err := b.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	b.Reset()
	if len(b.B) != 0 {
		t.Fatal("len(b.B) != 0")
	}
}

func TestByteBufferAcquireReleaseConcurrent(t *testing.T) {
	testBufferAcquireReleaseConcurrent(t, testByteBufferAcquireRelease)
}

func testBufferAcquireReleaseConcurrent(t *testing.T, c func(t *testing.T)) {
	concurrency := 10
	ch := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			c(t)
			ch <- struct{}{}
		}()
	}

	for i := 0; i < concurrency; i++ {
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("timeout!")
		}
	}
}

func TestIndexBufferAcquireReleaseConcurrent(t *testing.T) {
	testBufferAcquireReleaseConcurrent(t, testIndexBufferAcquireRelease)
}

type acquireF func() *ByteBuffer
type releaseF func(*ByteBuffer)

func testBufferAcquireRelease(t *testing.T, acquire acquireF, release releaseF) {
	for i := 0; i < 10; i++ {
		b := acquire()
		b.B = append(b.B, "num "...)
		b.B = fasthttp.AppendUint(b.B, i)
		expectedS := fmt.Sprintf("num %d", i)
		if string(b.B) != expectedS {
			t.Fatalf("unexpected result: %q. Expecting %q", b.B, expectedS)
		}
		release(b)
	}
}

func testByteBufferAcquireRelease(t *testing.T) {
	testBufferAcquireRelease(t,
		AcquireByteBuffer,
		ReleaseByteBuffer,
	)
}

func testIndexBufferAcquireRelease(t *testing.T) {
	testBufferAcquireRelease(t,
		AcquireIndexBuffer,
		ReleaseIndexBuffer,
	)
}
