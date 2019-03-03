package controllers

import (
	"crypto/sha256"
	"fmt"
)


func CCHash(s string) string{
	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("%s", s)))
	res := hasher.Sum(nil)
	return fmt.Sprintf("%x", string(res[0:4]))
}

func CCHashBlock(s string, pow string) string{
	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("%s%s", s, pow)))
	res := hasher.Sum(nil)
	return fmt.Sprintf("%x", string(res[0:4]))
}

