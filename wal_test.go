package wal

import (
	"fmt"
	"testing"
)

func TestOpen(t *testing.T) {
	wal, err := Open(DefaultOptions)
	fmt.Println(wal)
	fmt.Println(err)
}
