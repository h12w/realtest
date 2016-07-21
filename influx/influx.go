package influx

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"h12.me/realtest/container"
)

const (
	containerName = "realtest-influxdb-e09512beea0e4ecbb2909073ed1c03b1"
	internalPort  = 8086
)

type InfluxDB struct {
	c *container.Container
	client.Client
}

func New() (*InfluxDB, error) {
	c, err := container.FindOrCreate(containerName, "influxdb:latest")
	if err != nil {
		return nil, err
	}
	addr := "http://" + c.Addr(internalPort)
	influxClient, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:    addr,
		Timeout: time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &InfluxDB{
		c:      c,
		Client: influxClient,
	}, nil
}

func (d *InfluxDB) Addr() string {
	return d.c.Addr(internalPort)
}

func (d *InfluxDB) ID() string {
	return d.c.ID
}

func (d *InfluxDB) IP() string {
	return d.c.IP
}

func (d *InfluxDB) Name() string {
	return containerName
}

func (d *InfluxDB) CreateDatabase(dbName string) error {
	return d.exec(client.NewQuery("CREATE DATABASE "+dbName, "", ""))
}

func (d *InfluxDB) exec(q client.Query) error {
	resp, err := d.Client.Query(q)
	if err != nil {
		return err
	}
	return resp.Error()
}

func (d *InfluxDB) CreateRandomDatabase() (string, error) {
	dbName := RandomDBName()
	if err := d.CreateDatabase(dbName); err != nil {
		return "", err
	}
	return dbName, nil
}

func (d *InfluxDB) DeleteDatabase(dbName string) error {
	return d.exec(client.NewQuery("DROP DATABASE "+dbName, "", ""))
}

func RandomDBName() string {
	return "db_" + strconv.Itoa(rand.Int())
}
