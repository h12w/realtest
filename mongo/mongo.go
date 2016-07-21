package mongo

import (
	"math/rand"
	"strconv"

	"gopkg.in/mgo.v2"
	"h12.me/realtest/container"
)

const (
	containerName = "realtest-mongo-79cb399e9230494cb475d8461a0183c7"
	internalPort  = 27017
)

type Mongo struct {
	ConnStr string
	*mgo.Session
	*mgo.Database
	c *container.Container
}

func New() (*Mongo, error) {
	c, err := container.FindOrCreate(containerName, "mongo:latest")
	if err != nil {
		return nil, err
	}
	connStr := "mongodb://" + c.Addr(internalPort)
	session, err := mgo.Dial(connStr)
	if err != nil {
		c.Close()
		return nil, err
	}
	return &Mongo{
		ConnStr: connStr,
		Session: session,
		c:       c,
	}, nil
}

func (m *Mongo) NewDB(dbName string) *mgo.Database {
	return m.Session.DB(dbName)
}

func (m *Mongo) NewRandomDB() *mgo.Database {
	return m.Session.DB("db_" + strconv.Itoa(rand.Int()))
}

func (s *Mongo) Close() {
	if s.Session != nil {
		s.Session.Close()
		s.Session = nil
	}
}

func (s *Mongo) Addr() string {
	return s.c.Addr(internalPort)
}
