package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSubmitRuns(t *testing.T) {
	p := New(2, 4)
	defer func() { p.Close(); p.Wait() }()

	var n atomic.Int64
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if err := p.Submit(ctx, func() { n.Add(1) }); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()
	p.Wait()
	if n.Load() != 20 {
		t.Fatalf("got %d want 20", n.Load())
	}
}

func TestTrySubmitFull(t *testing.T) {
	p := New(1, 1)
	block := make(chan struct{})
	started := make(chan struct{})
	if err := p.Submit(context.Background(), func() {
		close(started)
		<-block
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.TrySubmit(func() {}); err != nil {
		t.Fatalf("queue should accept one: %v", err)
	}
	if err := p.TrySubmit(func() {}); err != ErrPoolFull {
		t.Fatalf("got %v want ErrPoolFull", err)
	}
	close(block)
	p.Close()
	p.Wait()
}

func TestPanicDoesNotKillWorker(t *testing.T) {
	p := New(1, 2)
	ctx := context.Background()
	if err := p.Submit(ctx, func() { panic("boom") }); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if err := p.Submit(ctx, func() { close(done) }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker died after panic")
	}
	p.Close()
	p.Wait()
}

func TestCloseRejectsSubmit(t *testing.T) {
	p := New(1, 1)
	p.Close()
	p.Wait()
	if err := p.Submit(context.Background(), func() {}); err != ErrPoolClosed {
		t.Fatalf("got %v want ErrPoolClosed", err)
	}
	if err := p.TrySubmit(func() {}); err != ErrPoolClosed {
		t.Fatalf("got %v want ErrPoolClosed", err)
	}
}

func TestCloseConcurrentSubmitNoPanic(t *testing.T) {
	p := New(4, 8)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = p.Submit(ctx, func() {})
		}()
	}
	time.Sleep(time.Millisecond)
	p.Close()
	wg.Wait()
	p.Wait()
}

func TestSubmitContextCancel(t *testing.T) {
	p := New(1, 0)
	defer func() { p.Close(); p.Wait() }()

	block := make(chan struct{})
	started := make(chan struct{})
	if err := p.Submit(context.Background(), func() {
		close(started)
		<-block
	}); err != nil {
		t.Fatal(err)
	}
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := p.Submit(ctx, func() {})
	if err != context.DeadlineExceeded {
		t.Fatalf("got %v want deadline", err)
	}
	close(block)
}

func TestSubmitErr(t *testing.T) {
	p := New(1, 2)
	defer func() { p.Close(); p.Wait() }()
	ch := SubmitErr(p, context.Background(), func() error { return ErrPoolFull })
	if err := <-ch; err != ErrPoolFull {
		t.Fatalf("got %v", err)
	}
}
