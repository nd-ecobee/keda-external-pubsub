package main

import (
	"strings"
)

func TrySend[T any](ch chan<- T, value T) bool {
	select {
	case ch <- value:
		return true
	default:
		return false
	}
}

func splitGCPResource(res string) []string {
	return strings.Split(res, "/")
}
