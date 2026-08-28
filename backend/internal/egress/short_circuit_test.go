//go:build unit

package egress

import "testing"

// Every billable upstream write site's enclosing function must short-circuit an
// authorization refusal (a call to AsAuthorizationRefused, or errors.Is against
// ErrAuthorizationRefused) — spec §2.0: "the branch list is a 3.3 deliverable".
func TestEveryBillableWriteSiteShortCircuitsRefusal(t *testing.T) {
	sites := mustLoadWriteSites(t)
	found := mustWalkWriteSites(t, repoRoot(t))
	for _, s := range sites {
		if s.Annotation != AnnotationBillable {
			continue
		}
		f, ok := found[s.Key()]
		if !ok {
			t.Errorf("pinned billable site %s no longer exists", s.Key())
			continue
		}
		if !f.EnclosingFuncShortCircuitsRefusal {
			t.Errorf("billable site %s: enclosing function %s has no AsAuthorizationRefused / ErrAuthorizationRefused check", s.Key(), f.Func)
		}
	}
}
