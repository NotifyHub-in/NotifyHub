package id

import (
	"crypto/rand"
	"encoding/hex"
)

func New(length int) string {
	buf := make([]byte, length)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
