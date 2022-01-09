package test

import (
	"testing"
	"zpbft/pbft"
)

func TestLog(t *testing.T) {
	pbft.Info("x = %d", 123)
	pbft.Info("x = 34")
	pbft.Warn("x = %d, y = %v", 123, []int{1, 2, 3})
	pbft.Error("y = %s", "abc")

}
