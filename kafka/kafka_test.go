package kafka

import (
	"fmt"
	"testing"
)

func TestIt(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(s.ZooKeeper.Addr())
	fmt.Println(s.Brokers())
}
