package fqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cenk/backoff"
	"github.com/kylelemons/godebug/pretty"
)

func TestQueue(t *testing.T) {
	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = time.Second * 10

	tests := []struct {
		name        string
		args        Args
		doers       []Doer
		doerCallErr []error
		waitErr     error
	}{
		{
			name: "Valid Arguments - Successful Doer Execution",
			args: Args{
				Limit:   2,
				Backoff: backoff.NewExponentialBackOff(),
			},
			doers: []Doer{
				func(ctx context.Context) error { return nil },
				func(ctx context.Context) error { return nil },
			},
			doerCallErr: []error{nil, nil},
			waitErr:     nil,
		},
		{
			name: "Error in Doer Execution",
			args: Args{
				Limit:      2,
				Backoff:    backoffPolicy,
				MaxRetries: 3, // or any other number that suits your needs
			},
			doers: []Doer{
				func(ctx context.Context) error { return errors.New("doer error") },
			},
			doerCallErr: []error{nil},
			waitErr:     errors.New("doer error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queue := New(tt.args)
			defer queue.Close()

			var lastErr error
			for i, doer := range tt.doers {
				lastErr = queue.Do(context.Background(), doer)
				if lastErr != tt.doerCallErr[i] {
					t.Errorf("Do() returned unexpected error: %v", lastErr)
				}
			}

			if diff := pretty.Compare(tt.waitErr, queue.Wait()); diff != "" {
				t.Errorf("Wait() returned unexpected error:\n%s", diff)
			}

		})
	}
}
