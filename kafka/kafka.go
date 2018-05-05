package kafka

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"

	"h12.io/realtest/container"
	"h12.io/realtest/zookeeper"
)

const (
	containerNameTemplate = "realtest-kafka-%d-1c750b5b00864c7cad7131bf7174c70e"
	clusterSize           = 5
	kafkaTopicsCmd        = "/opt/kafka/bin/kafka-topics.sh"
	internalPort          = 9092
	zkAddr                = "zk:2181/kafka"
)

var containerNames = func() (names []container.ContainerName) {
	for i := 0; i < clusterSize; i++ {
		names = append(names, container.ContainerName{Name: fmt.Sprintf(containerNameTemplate, i)})
	}
	return
}()

var mu sync.Mutex

type Cluster struct {
	*zookeeper.ZooKeeper
	cs []*container.Container
}

type MultiError struct {
	errors []error
	mu     sync.Mutex
}

func (e *MultiError) Add(err error) {
	e.mu.Lock()
	e.errors = append(e.errors, err)
	e.mu.Unlock()
}

func (e *MultiError) Error() string {
	return fmt.Sprint(e.errors)
}

func (e *MultiError) HasError() bool {
	return len(e.errors) > 0
}

func New() (*Cluster, error) {
	wg := new(sync.WaitGroup)
	merr := &MultiError{}
	var zk *zookeeper.ZooKeeper
	var err error
	zk, err = zookeeper.New()
	if err != nil {
		return nil, err
	}
	cs := make([]*container.Container, len(containerNames))
	for i := range cs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := containerNames[i].FindOrCreate("h12w/kafka:latest",
				fmt.Sprintf("--link=%s:zk", zk.Name()),
				"--env=KAFKA_DELETE_TOPIC_ENABLE=true",
				"--env=KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS=10000",
				"--env=KAFKA_OFFSETS_RETENTION_MINUTES=1",
				"--env=KAFKA_ADVERTISED_HOST_NAME="+zk.IP(),
				fmt.Sprintf("--env=KAFKA_BROKER_ID=%d", i),
				"--volume=/var/run/docker.sock:/var/run/docker.sock",
			)
			if err != nil {
				merr.Add(err)
			}
			cs[i] = c
		}(i)
	}
	wg.Wait()
	if merr.HasError() {
		return nil, merr
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
	cmd := k.anyNode().Command(kafkaTopicsCmd, "--zookeeper", zkAddr, "--create", "--topic", topic, "--partitions", strconv.Itoa(partition), "--replication-factor", "1")
	return cmd.Run()
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
