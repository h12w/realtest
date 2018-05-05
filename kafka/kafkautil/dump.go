package kafkautil

import (
	"encoding"
	"encoding/json"
	"sort"
	"strings"

	"h12.io/kpax/broker"
	"h12.io/kpax/cluster"
	"h12.io/kpax/consumer"
	"h12.io/kpax/proto"
	"h12.io/realtest/kafka"
)

func Dump(k *kafka.Cluster, topic string, newObj func() encoding.BinaryUnmarshaler) (string, error) {
	cl := cluster.New(broker.New, k.Brokers())
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
