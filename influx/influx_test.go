package influx

import (
	"fmt"
	"testing"
	"time"

	"github.com/influxdata/influxdb/client/v2"
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
	if err := s.Client.Write(testBp(db, t)); err != nil {
		t.Fatal(err)
	}
	dump, err := s.Dump(fmt.Sprintf(`SELECT * FROM %s."default".cpu_usage`, db), db)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(dump)
}

func testBp(db string, t *testing.T) client.BatchPoints {
	// Create a new point batch
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  db,
		Precision: "s",
	})

	// Create a point and add to batch
	tags := map[string]string{"cpu": "cpu-total"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.6,
	}
	pt, err := client.NewPoint("cpu_usage", tags, fields, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	bp.AddPoint(pt)
	return bp
}
