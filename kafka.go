package goframework

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	TopicConfiguration struct {
		Topic             string
		NumPartitions     int
		ReplicationFactor int
	}
)

func NewKafkaConfigMap(server string) *kafka.ConfigMap {
	return &kafka.ConfigMap{
		"bootstrap.servers": server,
		// "sasl.username":     v.GetString("username"),
		// "sasl.password":     v.GetString("password"),
	}
}

func NewKafkaAdminClient(cm *kafka.ConfigMap) *kafka.AdminClient {
	adm, err := kafka.NewAdminClient(cm)

	if err != nil {
		panic(err)
	}
	return adm
}

func CreateKafkaTopic(ctx context.Context,
	kcm *kafka.ConfigMap,
	tpc *TopicConfiguration) {

	admc := NewKafkaAdminClient(kcm)
	defer admc.Close()
	r, err := admc.CreateTopics(ctx,
		[]kafka.TopicSpecification{{
			Topic:             tpc.Topic,
			NumPartitions:     tpc.NumPartitions,
			ReplicationFactor: tpc.ReplicationFactor}},
		kafka.SetAdminOperationTimeout(time.Minute))

	if err != nil {
		panic(err)
	}

	if r[0].Error.Code() != kafka.ErrNoError &&
		r[0].Error.Code() != kafka.ErrTopicAlreadyExists {
		panic(r[0].Error.String())
	}
}
