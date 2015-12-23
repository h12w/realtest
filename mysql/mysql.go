package mysql

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"h12.me/realtest/container"
)

const (
	containerName = "realtest-mysql-f762b7f19a06403cb27bc8ab5f735840"
	internalPort  = 3306
)

const password = "1234"

type MySQL struct {
	ConnStr string
	*sql.DB
	c *container.Container
}

func (m *MySQL) CreateDatabase(dbName string) error {
	if _, err := m.DB.Exec("CREATE DATABASE " + dbName); err != nil {
		return err
	}
	if _, err := m.DB.Exec("USE " + dbName); err != nil {
		return err
	}
	return nil
}

func (m *MySQL) CreateRandomDatabase() (string, error) {
	dbName := RandomDBName()
	return dbName, m.CreateDatabase(dbName)
}

func RandomDBName() string {
	return "db_" + strconv.Itoa(rand.Int())
}

func New() (*MySQL, error) {
	c, err := container.Find(containerName)
	if err != nil {
		c, err = container.New("--name="+containerName, "--detach=true", "--publish-all=true", "--env=MYSQL_ROOT_PASSWORD="+password, "mysql:latest")
		if err != nil {
			return nil, err
		}
	}

	connStr := fmt.Sprintf("root:%s@tcp(%s)/", password, c.Addr(internalPort))
	x, err := sql.Open("mysql", connStr)
	if err != nil {
		c.Close()
		return nil, err
	}

	return &MySQL{
		ConnStr: connStr,
		DB:      x,
		c:       c,
	}, nil
}

func (m *MySQL) DeleteDatabase(dbName string) error {
	_, err := m.DB.Exec("DROP DATABASE " + dbName)
	return err
}

func (s *MySQL) Close() {
	if s.DB != nil {
		s.DB.Close()
		s.DB = nil
	}
}

func (s *MySQL) Addr() string {
	return s.c.Addr(internalPort)
}
