package zookeeper

import (
	"fmt"
	"testing"
)

func TestZK(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(s.Addr())
}
