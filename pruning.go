package coldstore

import "time"

// prunedByStats returns true when no non-null value in the segment can
// satisfy f according to min/max statistics.
func prunedByStats(sm segmentManifest, kind Kind, f Filter) bool {
	switch kind {
	case KindInt64:
		if sm.IMin == nil {
			return false
		}
		v, ok := asInt64(f.Value)
		if !ok {
			return false
		}
		switch f.Op {
		case OpEq:
			return v < *sm.IMin || v > *sm.IMax
		case OpNe:
			return false
		case OpLt, OpLe:
			if f.Op == OpLt {
				return *sm.IMin >= v
			}
			return *sm.IMin > v
		case OpGt, OpGe:
			if f.Op == OpGt {
				return *sm.IMax <= v
			}
			return *sm.IMax < v
		}
	case KindFloat64:
		if sm.FMin == nil {
			return false
		}
		v, ok := asFloat64(f.Value)
		if !ok {
			return false
		}
		switch f.Op {
		case OpEq:
			return v < *sm.FMin || v > *sm.FMax
		case OpNe:
			return false
		case OpLt:
			return *sm.FMin >= v
		case OpLe:
			return *sm.FMin > v
		case OpGt:
			return *sm.FMax <= v
		case OpGe:
			return *sm.FMax < v
		}
	case KindString:
		if sm.SMin == nil {
			return false
		}
		v, ok := f.Value.(string)
		if !ok {
			return false
		}
		switch f.Op {
		case OpEq:
			return v < *sm.SMin || v > *sm.SMax
		case OpNe:
			return false
		case OpLt:
			return *sm.SMin >= v
		case OpLe:
			return *sm.SMin > v
		case OpGt:
			return *sm.SMax <= v
		case OpGe:
			return *sm.SMax < v
		}
	case KindTimestamp:
		if sm.TMin == nil {
			return false
		}
		v, ok := f.Value.(time.Time)
		if !ok {
			return false
		}
		a, b := sm.TMin.UnixNano(), sm.TMax.UnixNano()
		x := v.UnixNano()
		switch f.Op {
		case OpEq:
			return x < a || x > b
		case OpNe:
			return false
		case OpLt:
			return a >= x
		case OpLe:
			return a > x
		case OpGt:
			return b <= x
		case OpGe:
			return b < x
		}
	}
	return false
}
