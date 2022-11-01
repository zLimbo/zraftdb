package zraft

import (
	"math"
	"time"
)

func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func MaxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func ToSecond(td time.Duration) float64 {
	return float64(td.Nanoseconds()) / math.Pow10(9)
}
