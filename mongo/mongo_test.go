package mongo

import (
	"testing"
)

func TestNewClose(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
}

func TestImport(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ImportCollection("testdb", "testc", `
{
	"a": 123,
	"b": 456
}
`); err != nil {
		t.Fatal(err)
	}
}
