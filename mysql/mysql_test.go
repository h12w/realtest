package mysql

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestMysql(t *testing.T) {
	s, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
}
