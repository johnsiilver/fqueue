/*
Package fqueue provides a type for allowing a function to finish before
the next one can be called. This is useful for things such as large database
updates where you only want to have a single update running at a time, but it
is okay to have multiple updates queued up as to not slow down processing with
some limit to keep memory usage in check.

Example usage:

	// Create a Queue with a limit of 1, which means you can only queue
	// up 1 Doer at a time. This has a backoff policy that will retry
	// the Doer if it returns an error with an exponential backoff.
	// It will only retry 10 times before giving up.
	q := New(
		Args{
			Limit:   1,
			Backoff: backoff.NewExponentialBackOff(),
			MaxRetries: 10, // or any other number that suits your needs
		},
	)

	for i := 0; i < 10; i++ {
		// Queue up a Doer to be executed.
		err := q.Do(context.Background(), func(ctx context.Context) error {
			// Do something that takes a while.
			return nil
		}
		if err != nil {
			break
		}
	}
	if err := q.Wait(); err != nil {
		// Handle the error.
	}
*/
package fqueue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/cenkalti/backoff/v4"
)

// Doer is a function that does something. Make sure that any Context that
// is passed has accounted for the wait time in the Queue. Generally, its
// a good idea to not have a timeout on the Context, but instead create a
// new Context with a timeout inside the Doer function. We pass the Context
// to the Doer so we can extract telemetry holders from it.
type Doer func(ctx context.Context) error

type exec struct {
	doer Doer
	ctx  context.Context
}

// Queue provides a type for queuing function calls to be executed sequentially
// while not having to wait for the previous call to finish. This is useful for
// things such as large database updates where you only want to have a single
// update running at a time, but it is okay to have multiple updates queued up
// to be executed after the previous one finishes.
type Queue struct {
	ch     chan exec
	err    atomic.Pointer[error]
	waiter *sync.WaitGroup
	closed chan struct{}
	args   Args
}

// Args are the arguments to New.
type Args struct {
	// Backoff provides a backoff implementation if you want the Queue
	// to retry Doers that return an error. Without this, a failure results
	// in the Queue becoming unusable and every subsequent Do() call will
	// return the encountered error. If this is nil, then this will use
	// backoff.StopBackOff, which acts as if no backoff is being used.
	Backoff backoff.BackOff
	// MaxRetries is the maximum number of times a Doer will be retried.
	// If this is <= 0, this will happen forever.
	MaxRetries int
	// Limit is the maximum number of Doers that can queue up before Do blocks.
	// If set to <= 0, then this will panic.
	Limit int
}

func (a Args) validate() error {
	if a.Limit < 1 {
		return errors.New("limit must be at least 1")
	}

	return nil
}

// New creates a new Queue with the given limit.
// If the Args do not validate, then this will panic.
func New(args Args) *Queue {
	if err := args.validate(); err != nil {
		panic(err)
	}

	if args.Backoff == nil {
		args.Backoff = &backoff.StopBackOff{}
	}

	l := &Queue{
		args:   args,
		ch:     make(chan exec, args.Limit),
		waiter: new(sync.WaitGroup),
		closed: make(chan struct{}),
	}

	go l.handle()
	return l
}

// Close closes the Queue. This can only be called once or it will panic.
// Calling this without waiting for all queued Doers to finish results
// in undefined behavior.
func (l *Queue) Close() {
	close(l.closed)
}

func (l *Queue) handle() {
	hadErr := false
	for {
		select {
		case <-l.closed:
			close(l.ch)
			for range l.ch {
				// Drain the channel.
			}
			return
		case exec := <-l.ch:
			hadErr = l.op(hadErr, exec)
		}
	}
}

func (l *Queue) op(hadErr bool, exec exec) bool {
	defer l.waiter.Done()

	if hadErr {
		return hadErr
	}

	var realErr error
	retryCount := 0
	op := func() error {
		if l.args.MaxRetries >= 0 {
			if retryCount > l.args.MaxRetries {
				return backoff.Permanent(errors.New("max retries reached"))
			}
		}
		retryCount++
		realErr = exec.doer(exec.ctx)
		return realErr
	}

	err := backoff.Retry(
		op,
		backoff.WithContext(
			l.args.Backoff,
			exec.ctx,
		),
	)
	if err != nil {
		l.err.Store(&realErr)
		return true
	}
	return false
}

// Do queues a Doer to be called. If the queue is full, then this will block.
// If the Context is canceled, then this will return the Context's error.
// If the Doer returns an error, then this will retry the Doer according to
// the Args. If the Doer runs out of retries or the Context is canceled, then
// the Queue will become unusable and return errors on every Do call.
// You can check the error status of the Queue by calling the Err method.
// This is safe to call concurrently.
func (l *Queue) Do(ctx context.Context, doer Doer) error {
	if e := l.err.Load(); e != nil {
		return *e
	}

	e := exec{
		doer: doer,
		ctx:  ctx,
	}
	l.waiter.Add(1) // This is decremented in the handle function.

	select {
	case <-ctx.Done():
		l.waiter.Done() // Decrement immediately since it won't be handled.
		return ctx.Err()
	case l.ch <- e:
		return nil
	}
}

// Err returns the error status of the Queue. If the Queue is still usable,
// then this will return nil. If the Queue is unusable, then this will return
// the error that caused the Queue to become unusable.
// This is safe to call concurrently.
func (l *Queue) Err() error {
	v := l.err.Load()
	if v == nil {
		return nil
	}
	return *v
}

// Wait waits for all queued Doers to finish. If the Queue is unusable, then
// this will return the error that caused the Queue to become unusable.
// This is safe to call concurrently.
func (l *Queue) Wait() error {
	l.waiter.Wait()
	return l.Err()
}
