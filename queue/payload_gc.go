package queue

import (
	"context"
	"errors"

	"github.com/damianb/s3collections/tree"
)

// runPayloadGC resumes every persisted payload-store plan before creating a
// new one. The plans themselves are durable, so a crash between PlanGC and
// maintenance bookkeeping cannot leak unreachable payloads.
func (q *Queue) runPayloadGC(ctx context.Context) {
	plans, err := q.payloads.ListGCPlans(ctx)
	if err != nil {
		q.opts.Logger.Warn(err, "queue: list payload GC plans failed")
		return
	}
	hadPlan := len(plans) > 0
	for _, plan := range plans {
		if q.now().Before(plan.NotBefore) {
			continue
		}
		result, e := q.payloads.SweepGC(ctx, plan)
		if e != nil {
			if !errors.Is(e, tree.ErrPlanNotReady) {
				q.opts.Logger.Warn(e, "queue: payload GC sweep failed", "plan", plan.ID)
			}
			continue
		}
		q.opts.Logger.Info("queue: payload GC swept", "plan", plan.ID, "nodes", result.NodesDeleted, "blobs", result.BlobsDeleted)
	}
	if hadPlan {
		return
	}
	cutoff := q.now().Add(-q.opts.PayloadGCGrace)
	if _, err = q.payloads.PlanGC(ctx, cutoff); err != nil {
		q.opts.Logger.Warn(err, "queue: payload GC plan failed")
	}
}
