package graph

import (
	"context"
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// TestDiamondGroupingDeduplicatesSharedSubproblem exercises a diamond-shaped
// group hierarchy: g1 and g2 both contain g3, and the checked user reaches
// g3#member through a single leaf group g4. Without in-flight deduplication the
// g4#member subproblem is resolved once per branch, doubling datastore reads;
// with it, the second branch joins the first branch's in-flight resolution.
func TestDiamondGroupingDeduplicatesSharedSubproblem(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	storeID := ulid.Make().String()
	model := parser.MustTransformDSLToProto(`
		model
			schema 1.1
		type user
		type group
			relations
				define member: [user, group#member]
	`)

	ts, err := typesystem.New(model)
	require.NoError(t, err)

	restrictions, err := ts.DirectlyRelatedUsersets("group", "member")
	require.NoError(t, err)

	ds := mocks.NewMockRelationshipTupleReader(ctrl)

	// g1 -> {g2, g3}, g2 -> {g3}, g3 -> {g4}, g4 -> {user:maria}.
	usersetTuples := map[string][]*openfgav1.Tuple{
		"group:g1": {
			{Key: tuple.NewTupleKey("group:g1", "member", "group:g2#member")},
			{Key: tuple.NewTupleKey("group:g1", "member", "group:g3#member")},
		},
		"group:g2": {
			{Key: tuple.NewTupleKey("group:g2", "member", "group:g3#member")},
		},
		"group:g3": {
			{Key: tuple.NewTupleKey("group:g3", "member", "group:g4#member")},
		},
		"group:g4": {},
	}

	readUsersetCalls := make(map[string]int)
	readUsersetMu := sync.Mutex{}
	ds.EXPECT().
		ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		MaxTimes(len(usersetTuples)).
		DoAndReturn(func(_ context.Context, _ string, filter storage.ReadUsersetTuplesFilter, _ storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
			require.Equal(t, "member", filter.Relation)
			require.Equal(t, restrictions, filter.AllowedUserTypeRestrictions)
			readUsersetMu.Lock()
			readUsersetCalls[filter.Object]++
			readUsersetMu.Unlock()
			return storage.NewStaticTupleIterator(usersetTuples[filter.Object]), nil
		})

	// Every node probes for a direct user tuple. The leaf group matches; the
	// others return ErrNotFound and continue down the userset branch.
	ds.EXPECT().
		ReadUserTuple(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ string, filter storage.ReadUserTupleFilter, _ storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
			if filter.Object == "group:g4" && filter.User == "user:maria" {
				return &openfgav1.Tuple{Key: tuple.NewTupleKey("group:g4", "member", "user:maria")}, nil
			}
			return nil, storage.ErrNotFound
		})

	ctx := setRequestContext(context.Background(), ts, ds, nil)

	checker := NewLocalChecker()
	t.Cleanup(checker.Close)

	req := &ResolveCheckRequest{
		StoreID:              storeID,
		AuthorizationModelID: model.GetId(),
		TupleKey:             tuple.NewTupleKey("group:g1", "member", "user:maria"),
		RequestMetadata:      NewCheckRequestMetadata(),
		SelectedStrategy:     defaultResolver,
		inflight:             newInflightSubproblemRegistry(),
	}

	resp, err := checker.ResolveCheck(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.GetAllowed())

	// Each userset node is expanded exactly once: g3 is reached from two branches but resolved once.
	readUsersetMu.Lock()
	defer readUsersetMu.Unlock()
	for object, calls := range readUsersetCalls {
		require.Equal(t, 1, calls, "expected a single ReadUsersetTuples for %s", object)
	}
	require.Equal(t, 1, readUsersetCalls["group:g3"], "shared g3 subproblem must be resolved once despite two incoming branches")
}
