package columnar

import "math"

func float64FromBits(bits uint64) float64 { return math.Float64frombits(bits) }

func maxInt() int { return int(^uint(0) >> 1) }
