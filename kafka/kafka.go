package kafka

import (
	"encoding"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"

	"h12.me/kpax/broker"
	"h12.me/kpax/cluster"
	"h12.me/kpax/consumer"
	"h12.me/kpax/proto"
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
	cs := make([]*container.Container, clusterSize)
	for i := range cs {
		containerName := fmt.Sprintf(containerNameTemplate, i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := findOrCreateKafka(containerName, i, zk)
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

func findOrCreateKafka(containerName string, i int, zk *zookeeper.ZooKeeper) (*container.Container, error) {
	c, err := container.Find(containerName)
	if err != nil {
		c, err = container.New("--name="+containerName, "--detach=true", "--publish-all=true",
			fmt.Sprintf("--link=%s:zk", zk.Name()),
			"--env=KAFKA_DELETE_TOPIC_ENABLE=true",
			"--env=KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS=10000",
			"--env=KAFKA_OFFSETS_RETENTION_MINUTES=1",
			"--env=KAFKA_ADVERTISED_HOST_NAME="+zk.IP(),
			"--env=KAFKA_ADVERTISED_PORT=9092",
			fmt.Sprintf("--env=KAFKA_BROKER_ID=%d", i),
			"--volume=/var/run/docker.sock:/var/run/docker.sock",
			"h12w/kafka:latest",
		)
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	return c, nil
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

func (k *Cluster) Dump(topic string, newObj func() encoding.BinaryUnmarshaler) (string, error) {
	cl := cluster.New(broker.NewDefault, k.Brokers())
	partitions, err := cl.Partitions(topic)
	if err != nil {
		return "", err
	}
	cr := consumer.New(cl)
	var lines []string
	for _, partition := range partitions {
		start, err := cr.FetchOffsetByTime(topic, partition, proto.Earliest)
		if err != nil {
			return "", err
		}
		end, err := cr.FetchOffsetByTime(topic, partition, proto.Latest)
		if err != nil {
			return "", err
		}
		for offset := start; offset < end; {
			messages, err := cr.Consume(topic, partition, offset)
			if err != nil {
				return "", err
			}
			if len(messages) == 0 {
				break
			}
			for _, message := range messages {
				obj := newObj()
				if err := obj.UnmarshalBinary(message.Value); err != nil {
					return "", err
				}
				jsonBuf, err := json.MarshalIndent(obj, "", "\t")
				if err != nil {
					return "", err
				}
				lines = append(lines, string(jsonBuf))
			}
			offset += messages[len(messages)-1].Offset + 1
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n"), nil
}

func RandomGroup() string {
	return "group_" + strconv.Itoa(rand.Int())
}
