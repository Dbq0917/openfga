package graph

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func waitUntil(condition func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return condition()
}

func TestInflightRegistryDeduplicatesInFlightSubproblem(t *testing.T) {
	t.Parallel()

	registry := newInflightSubproblemRegistry()
	const key = "group:g4#member@user:maria"

	var executions atomic.Int32
	releaseLeader := make(chan struct{})

	leaderDone := make(chan struct{})
	go func() {
		_, _ = registry.resolve(context.Background(), key, func() (*ResolveCheckResponse, error) {
			executions.Add(1)
			<-releaseLeader
			return &ResolveCheckResponse{Allowed: true}, nil
		})
		close(leaderDone)
	}()

	require.True(t, waitUntil(func() bool {
		return registry.hasInFlight(key)
	}, time.Second), "leader never registered its in-flight call")

	const waiters = 15
	var waiterWg sync.WaitGroup
	waiterWg.Add(waiters)
	waiterReturned := make(chan struct{})
	for range waiters {
		go func() {
			defer waiterWg.Done()
			resp, err := registry.resolve(context.Background(), key, func() (*ResolveCheckResponse, error) {
				executions.Add(1)
				return &ResolveCheckResponse{Allowed: true}, nil
			})
			require.NoError(t, err)
			require.True(t, resp.GetAllowed())
		}()
	}
	require.True(t, waitUntil(func() bool {
		return registry.waiterCount(key) == waiters
	}, time.Second), "waiters did not join the in-flight call")

	go func() {
		waiterWg.Wait()
		close(waiterReturned)
	}()

	// While the leader is blocked, waiters must join its in-flight call instead of running their own.
	select {
	case <-waiterReturned:
		t.Fatal("waiter returned before the leader completed")
	case <-time.After(50 * time.Millisecond):
	}
	require.Equal(t, int32(1), executions.Load())

	close(releaseLeader)

	select {
	case <-waiterReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("waiters did not return after leader completion")
	}
	<-leaderDone
	require.Equal(t, int32(1), executions.Load())
}

func TestInflightRegistryRetriesAfterCompletion(t *testing.T) {
	t.Parallel()

	registry := newInflightSubproblemRegistry()

	var executions atomic.Int32
	run := func() {
		resp, err := registry.resolve(context.Background(), "k", func() (*ResolveCheckResponse, error) {
			executions.Add(1)
			return &ResolveCheckResponse{Allowed: false}, nil
		})
		require.NoError(t, err)
		require.False(t, resp.GetAllowed())
	}

	run()
	run()
	require.Equal(t, int32(2), executions.Load())
}

func TestInflightRegistryPropagatesErrorsToWaiters(t *testing.T) {
	t.Parallel()

	registry := newInflightSubproblemRegistry()
	boom := errors.New("boom")

	releaseLeader := make(chan struct{})

	go func() {
		_, _ = registry.resolve(context.Background(), "k", func() (*ResolveCheckResponse, error) {
			<-releaseLeader
			return nil, boom
		})
	}()

	require.True(t, waitUntil(func() bool {
		return registry.hasInFlight("k")
	}, time.Second), "leader never registered its in-flight call")

	const waiters = 8
	var waiterWg sync.WaitGroup
	waiterWg.Add(waiters)
	for range waiters {
		go func() {
			defer waiterWg.Done()
			_, err := registry.resolve(context.Background(), "k", func() (*ResolveCheckResponse, error) {
				return nil, errors.New("should not run")
			})
			require.ErrorIs(t, err, boom)
		}()
	}

	require.True(t, waitUntil(func() bool {
		return registry.waiterCount("k") == waiters
	}, time.Second), "waiters did not join the in-flight call")

	close(releaseLeader)

	done := make(chan struct{})
	go func() {
		waiterWg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("waiters did not return after error")
	}
}

func TestInflightRegistryDistinctKeysRunIndependently(t *testing.T) {
	t.Parallel()

	registry := newInflightSubproblemRegistry()

	var executions atomic.Int32
	var wg sync.WaitGroup
	for _, key := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			_, err := registry.resolve(context.Background(), key, func() (*ResolveCheckResponse, error) {
				executions.Add(1)
				return &ResolveCheckResponse{}, nil
			})
			require.NoError(t, err)
		}(key)
	}
	wg.Wait()
	require.Equal(t, int32(3), executions.Load())
}

func TestInflightRegistryWaiterReturnsOnContextCancellation(t *testing.T) {
	t.Parallel()

	registry := newInflightSubproblemRegistry()

	releaseLeader := make(chan struct{})
	go func() {
		_, _ = registry.resolve(context.Background(), "k", func() (*ResolveCheckResponse, error) {
			<-releaseLeader
			return &ResolveCheckResponse{Allowed: true}, nil
		})
	}()

	require.True(t, waitUntil(func() bool {
		return registry.hasInFlight("k")
	}, time.Second), "leader never registered its in-flight call")

	waiterCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := registry.resolve(waiterCtx, "k", func() (*ResolveCheckResponse, error) {
			return nil, errors.New("waiter must not run its own resolution")
		})
		errCh <- err
	}()

	require.True(t, waitUntil(func() bool {
		return registry.waiterCount("k") == 1
	}, time.Second), "waiter did not join the in-flight call")

	cancel()
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not return after context cancellation")
	}

	// The leader is unaffected by the cancelled waiter and still resolves successfully.
	close(releaseLeader)
	require.True(t, waitUntil(func() bool {
		return !registry.hasInFlight("k")
	}, time.Second), "leader did not complete after release")
}
