package cloudspanner

import (
	"math/bits"
)

func spannerKey(n uint32) uint32 {
	return bits.Reverse32(n)
}
