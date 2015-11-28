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
	topic, err := s.NewRandomTopic(3)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(topic)
	fmt.Println(s.DescribeTopic(topic))
	if err := s.DeleteTopic(topic); err != nil {
		t.Fatal(err)
	}
}
