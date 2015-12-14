package kafka

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"h12.me/realtest/container"
	"h12.me/realtest/zookeeper"
)

const (
	containerNameTemplate = "realtest-kafka-%d-1c750b5b00864c7cad7131bf7174c70e"
	clusterSize           = 5
	kafkaTopicsCmd        = "/opt/kafka/bin/kafka-topics.sh"
	internalPort          = 9092
	zkAddr                = "zk:2181/kafka"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Cluster struct {
	*zookeeper.ZooKeeper
	cs []*container.Container
}

func New() (*Cluster, error) {
	zk, err := zookeeper.New()
	if err != nil {
		return nil, err
	}
	cs := make([]*container.Container, clusterSize)
	for i := 0; i < clusterSize; i++ {
		containerName := fmt.Sprintf(containerNameTemplate, i)
		c, err := container.Find(containerName)
		if err != nil {
			c, err = container.New("--name="+containerName, "--detach=true", "--publish-all=true",
				fmt.Sprintf("--link=%s:zk", zk.Name()),
				"--env=KAFKA_DELETE_TOPIC_ENABLE=true",
				"--env=KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS=1",
				"--env=KAFKA_OFFSETS_RETENTION_MINUTES=1",
				"--env=KAFKA_ADVERTISED_HOST_NAME="+zk.IP(),
				fmt.Sprintf("--env=KAFKA_BROKER_ID=%d", i),
				"--volume=/var/run/docker.sock:/var/run/docker.sock",
				"h12w/kafka:latest",
			)
			if err != nil {
				return nil, err
			}
		}
		cs[i] = c
	}
	return &Cluster{
		ZooKeeper: zk,
		cs:        cs,
	}, nil
}

func (k *Cluster) anyNode() *container.Container {
	return k.cs[rand.Intn(len(k.cs))]
}

func (k *Cluster) NewTopic(topic string, partition int) error {
	return k.anyNode().Command(kafkaTopicsCmd, "--zookeeper", zkAddr, "--create", "--topic", topic, "--partitions", strconv.Itoa(partition), "--replication-factor", "1").Run()
}

func (k *Cluster) NewRandomTopic(partition int) (string, error) {
	topic := "topic_" + strconv.Itoa(rand.Int())
	return topic, k.NewTopic(topic, partition)
}

func (k *Cluster) DeleteTopic(topic string) error {
	return k.anyNode().Command(kafkaTopicsCmd, "--zookeeper", zkAddr, "--delete", "--topic", topic).Run()
}

func (k *Cluster) DescribeTopic(topic string) string {
	return string(k.anyNode().Command(kafkaTopicsCmd, "--zookeeper", zkAddr, "--describe", "--topic", topic).Output())
}

func (k *Cluster) Brokers() []string {
	res := make([]string, len(k.cs))
	for i, c := range k.cs {
		res[i] = c.Addr(internalPort)
	}
	return res
}

func (k *Cluster) AnyBroker() string {
	return k.anyNode().Addr(internalPort)
}

func RandomGroup() string {
	return "group_" + strconv.Itoa(rand.Int())
}
