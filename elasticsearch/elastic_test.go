package elasticsearch

import "testing"

func TestElastic(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteIndex("idx-test"); err != nil {
		t.Fatal(err)
	}
}
