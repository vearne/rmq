package rmq

import (
	"crypto/sha1"
	"fmt"
	"io"
)

func CalcuSHA1(msg string) string {
	h := sha1.New()
	io.WriteString(h, msg)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
