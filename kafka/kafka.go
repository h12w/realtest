package kafka

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"h12.me/realtest/container"
	"h12.me/realtest/util"
	"h12.me/realtest/zookeeper"
)

const (
	containerNameTemplate = "realtest-kafka-%d-1c750b5b00864c7cad7131bf7174c70e"
	clusterSize           = 5
	kafkaTopicsCmd        = "kafka-topics.sh"
	internalPort          = 9092
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type KafkaCluster struct {
	*zookeeper.ZooKeeper
	cs []*container.Container
}

func New() (*KafkaCluster, error) {
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
				"--env=KAFKA_ADVERTISED_HOST_NAME="+zk.ID(),
				fmt.Sprintf("--env=KAFKA_BROKER_ID=%d", i),
				"--volume=/var/run/docker.sock:/var/run/docker.sock",
				"h12w/kafka",
			)
			if err != nil {
				return nil, err
			}
		}
		cs[i] = c
	}
	return &KafkaCluster{
		ZooKeeper: zk,
		cs:        cs,
	}, nil
}

func (k *KafkaCluster) NewTopic(partition int) (string, error) {
	if !util.CmdExists(kafkaTopicsCmd) {
		return "", errors.New(kafkaTopicsCmd + " not found in path, please install Kafka first")
	}
	topic := "topic_" + strconv.Itoa(rand.Int())
	return topic, util.Command(kafkaTopicsCmd, "--zookeeper", k.ZooKeeper.Addr(), "--create", "--topic", topic, "--partitions", strconv.Itoa(partition), "--replication-factor", "1").Run()
}

func (k *KafkaCluster) DeleteTopic(topic string) error {
	return util.Command(kafkaTopicsCmd, "--zookeeper", k.ZooKeeper.Addr(), "--delete", "--topic", topic).Run()
}

func (k *KafkaCluster) DescribeTopic(topic string) string {
	return string(util.Command(kafkaTopicsCmd, "--zookeeper", k.ZooKeeper.Addr(), "--describe", "--topic", topic).Output())
}

func (k *KafkaCluster) Brokers() []string {
	res := make([]string, len(k.cs))
	for i, c := range k.cs {
		res[i] = c.Addr(internalPort)
	}
	return res
}
