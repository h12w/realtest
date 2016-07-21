package influx

import (
	"testing"
)

func TestInflux(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	db, err := s.CreateRandomDatabase()
	if err != nil {
		t.Fatal(err)
	}
	defer s.DeleteDatabase(db)
}
