package graph

import (
	"context"
	"sync"
)

// inflightSubproblemCall represents a subproblem that is currently being
// resolved by exactly one goroutine. Concurrent callers with the same key
// block on done and reuse the leader's response instead of re-walking the
// relationship graph and re-querying the datastore.
type inflightSubproblemCall struct {
	done chan struct{}
	resp *ResolveCheckResponse
	err  error
	// waiters counts callers blocked on done. It is only used to make tests
	// deterministic; production code does not branch on it.
	waiters int
}

// inflightSubproblemRegistry deduplicates identical Check subproblems within
// a single top-level request. It holds no data after each call completes, so it
// does not cache authorization results across requests.
type inflightSubproblemRegistry struct {
	mu      sync.Mutex
	pending map[string]*inflightSubproblemCall
}

func newInflightSubproblemRegistry() *inflightSubproblemRegistry {
	return &inflightSubproblemRegistry{
		pending: make(map[string]*inflightSubproblemCall),
	}
}

func (r *inflightSubproblemRegistry) hasInFlight(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.pending[key]
	return ok
}

func (r *inflightSubproblemRegistry) waiterCount(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if call, ok := r.pending[key]; ok {
		return call.waiters
	}
	return 0
}

// resolve runs fn for key. If another goroutine is already resolving the same
// key, it waits for that resolution and returns its response. A completed call
// is removed from the registry before waiters are released, so subsequent
// invocations are resolved independently rather than served as a cache.
//
// A waiter whose context is cancelled before the leader finishes returns the
// context error without unregistering the leader, so other branches and future
// callers can still consume the in-flight resolution.
func (r *inflightSubproblemRegistry) resolve(
	ctx context.Context,
	key string,
	fn func() (*ResolveCheckResponse, error),
) (*ResolveCheckResponse, error) {
	r.mu.Lock()
	if call, ok := r.pending[key]; ok {
		call.waiters++
		r.mu.Unlock()
		select {
		case <-call.done:
			return call.resp, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	call := &inflightSubproblemCall{done: make(chan struct{})}
	r.pending[key] = call
	r.mu.Unlock()

	call.resp, call.err = fn()

	r.mu.Lock()
	delete(r.pending, key)
	r.mu.Unlock()

	close(call.done)
	return call.resp, call.err
}
