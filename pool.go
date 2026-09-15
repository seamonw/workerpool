// Package workerpool is a fixed-size goroutine pool with a bounded queue.
//
// It exists as a concurrency limiter, not a cheaper substitute for go f().
// See https://seamonw.github.io/blog/2026/09/14/golang-worker-pool/
package workerpool

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrPoolClosed = errors.New("pool: closed")
	ErrPoolFull   = errors.New("pool: full")
	ErrNilTask    = errors.New("pool: nil task")
)

// Pool runs at most workers tasks at a time. Extra tasks wait in a buffered
// channel of size queue. Close stops new submits and Wait drains in-flight work.
type Pool struct {
	jobs   chan func()
	wg     sync.WaitGroup
	mu     sync.Mutex
	closed bool
}

func New(workers, queue int) *Pool {
	if workers <= 0 {
		workers = 1
	}
	if queue < 0 {
		queue = 0
	}
	p := &Pool{jobs: make(chan func(), queue)}
	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for fn := range p.jobs {
		run(fn)
	}
}

func run(fn func()) {
	defer func() { _ = recover() }()
	fn()
}

// Submit blocks until the task is queued, ctx is done, or the pool is closed.
// mu is held across the send so Close cannot close jobs while a send is in flight.
func (p *Pool) Submit(ctx context.Context, fn func()) error {
	if fn == nil {
		return ErrNilTask
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	select {
	case p.jobs <- fn:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pool) TrySubmit(fn func()) error {
	if fn == nil {
		return ErrNilTask
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolClosed
	}
	select {
	case p.jobs <- fn:
		return nil
	default:
		return ErrPoolFull
	}
}

func (p *Pool) SubmitCtx(ctx context.Context, fn func(context.Context)) error {
	if fn == nil {
		return ErrNilTask
	}
	return p.Submit(ctx, func() { fn(ctx) })
}

func SubmitErr(p *Pool, ctx context.Context, fn func() error) <-chan error {
	ch := make(chan error, 1)
	if fn == nil {
		ch <- ErrNilTask
		return ch
	}
	err := p.Submit(ctx, func() { ch <- fn() })
	if err != nil {
		ch <- err
	}
	return ch
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.jobs)
}

func (p *Pool) Wait() { p.wg.Wait() }
