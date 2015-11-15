package zookeeper

import (
	"h12.me/realtest/container"
)

const (
	containerName = "realtest-zookeeper-a34ea1a3e95244de8f278c79e9b2cb94"
	internalPort  = 2181
)

type ZooKeeper struct {
	c *container.Container
}

func New() (*ZooKeeper, error) {
	c, err := container.Find(containerName)
	if err != nil {
		c, err = container.New("--name="+containerName, "--detach=true", "--publish-all=true", "h12w/zookeeper:latest")
		if err != nil {
			return nil, err
		}
	}
	return &ZooKeeper{
		c: c,
	}, nil
}

func (s *ZooKeeper) Addr() string {
	return s.c.Addr(internalPort)
}

func (s *ZooKeeper) ID() string {
	return s.c.ID
}

func (s *ZooKeeper) IP() string {
	return s.c.IP
}

func (s *ZooKeeper) Name() string {
	return containerName
}
