package coldstore

import (
	"encoding/json"
	"time"
)

const manifestName = "manifest.json"

type manifest struct {
	Version int             `json:"version"`
	Columns []Column        `json:"columns"`
	Batches []batchManifest `json:"batches"`
}

type batchManifest struct {
	ID       int                        `json:"id"`
	Rows     int                        `json:"rows"`
	Segments map[string]segmentManifest `json:"segments"`
}

type segmentManifest struct {
	File    string     `json:"file"`
	Nulls   int        `json:"nulls"`
	NonNull int        `json:"non_null"`
	IMin    *int64     `json:"imin,omitempty"`
	IMax    *int64     `json:"imax,omitempty"`
	FMin    *float64   `json:"fmin,omitempty"`
	FMax    *float64   `json:"fmax,omitempty"`
	SMin    *string    `json:"smin,omitempty"`
	SMax    *string    `json:"smax,omitempty"`
	TMin    *time.Time `json:"tmin,omitempty"`
	TMax    *time.Time `json:"tmax,omitempty"`
}

func (m *manifest) schema() Schema {
	return Schema{Columns: append([]Column(nil), m.Columns...)}
}

func encodeManifest(m *manifest) ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

func decodeManifest(raw []byte) (*manifest, error) {
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func statsFromColumn(data ColumnData) segmentManifest {
	sm := segmentManifest{Nulls: 0, NonNull: 0}
	n := data.Len()
	for i := 0; i < n; i++ {
		if data.IsNull(i) {
			sm.Nulls++
			continue
		}
		sm.NonNull++
		switch data.Kind {
		case KindInt64:
			v := data.Int64s[i]
			if sm.IMin == nil || v < *sm.IMin {
				sm.IMin = newInt64(v)
			}
			if sm.IMax == nil || v > *sm.IMax {
				sm.IMax = newInt64(v)
			}
		case KindFloat64:
			v := data.Float64s[i]
			if sm.FMin == nil || v < *sm.FMin {
				sm.FMin = newFloat64(v)
			}
			if sm.FMax == nil || v > *sm.FMax {
				sm.FMax = newFloat64(v)
			}
		case KindString:
			v := data.Strings[i]
			if sm.SMin == nil || v < *sm.SMin {
				sm.SMin = newString(v)
			}
			if sm.SMax == nil || v > *sm.SMax {
				sm.SMax = newString(v)
			}
		case KindTimestamp:
			v := data.Timestamps[i].UTC()
			if sm.TMin == nil || v.Before(*sm.TMin) {
				t := v
				sm.TMin = &t
			}
			if sm.TMax == nil || v.After(*sm.TMax) {
				t := v
				sm.TMax = &t
			}
		}
	}
	return sm
}

func newInt64(v int64) *int64       { return &v }
func newFloat64(v float64) *float64 { return &v }
func newString(v string) *string    { return &v }
