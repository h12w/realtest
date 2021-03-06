package redis

import (
	"time"

	"github.com/garyburd/redigo/redis"
	"h12.io/realtest/container"
)

const (
	containerName = "realtest-redis-ac3bfb841b3c47378dfdecca51b23042"
	internalPort  = 6379
)

type Redis struct {
	pool *redis.Pool
	c    *container.Container
}

func New() (*Redis, error) {
	c, err := container.FindOrCreate(containerName, "redis:latest")
	if err != nil {
		return nil, err
	}
	return &Redis{
		pool: &redis.Pool{
			MaxIdle:     3,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", c.Addr(internalPort))
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
		c: c,
	}, nil
}

func (s *Redis) Pool() *redis.Pool {
	return s.pool
}

func (s *Redis) Addr() string {
	return s.c.Addr(internalPort)
}
