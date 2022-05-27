package configs

import "math/rand"

const (
	NumUsers = 100000
)

func RandUserId() int {
	return rand.Intn(NumUsers/2) + rand.Intn(NumUsers/2) + 1
}
