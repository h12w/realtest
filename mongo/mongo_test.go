package mongo

import (
	"testing"

	"github.com/mongodb/mongo-tools/mongoimport"
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

}
