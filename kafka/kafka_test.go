package kafka

import (
	"testing"
)

func TestIt(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	_ = s
	t.Log(s.ZooKeeper.Addr())
	t.Log(s.Brokers())
	topic, err := s.NewRandomTopic(3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := s.DeleteTopic(topic); err != nil {
			t.Fatal(err)
		}
	}()
	t.Log(topic)
	t.Log(s.DescribeTopic(topic))
}
