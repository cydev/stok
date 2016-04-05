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
	concurrency := 10
	ch := make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			testByteBufferAcquireRelease(t)
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

func testByteBufferAcquireRelease(t *testing.T) {
	for i := 0; i < 10; i++ {
		b := AcquireByteBuffer()
		b.B = append(b.B, "num "...)
		b.B = fasthttp.AppendUint(b.B, i)
		expectedS := fmt.Sprintf("num %d", i)
		if string(b.B) != expectedS {
			t.Fatalf("unexpected result: %q. Expecting %q", b.B, expectedS)
		}
		ReleaseByteBuffer(b)
	}
}
