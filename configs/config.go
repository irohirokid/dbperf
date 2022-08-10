package configs

import "math/rand"

const (
	NumUsers = 100000
)

func RandUserId() uint32 {
	return uint32(rand.Intn(NumUsers/2) + rand.Intn(NumUsers/2) + 1)
}
