package goframework

import (
	"context"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	TopicConfiguration struct {
		Topic             string
		NumPartitions     int
		ReplicationFactor int
	}

	GoKafka struct {
		server  string
		groupId string
	}
)

func NewKafkaConfigMap(connectionString string, groupId string) *GoKafka {
	return &GoKafka{
		server:  connectionString,
		groupId: groupId,
	}
}

func (k *GoKafka) Consumer(topic string, fn ConsumerFunc) {
	go func(topic string) {

		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: "earliest",
			Retries:         5,
		}

		kc := &kafka.ConfigMap{
			"bootstrap.servers": k.server,
			"group.id":          k.groupId,
			"auto.offset.reset": kcs.AutoOffsetReset}

		// CreateKafkaTopic(context.Background(), kc, &TopicConfiguration{
		// 	Topic:             kcs.Topic,
		// 	NumPartitions:     kcs.NumPartitions,
		// 	ReplicationFactor: kcs.ReplicationFactor,
		// })

		consumer, err := kafka.NewConsumer(kc)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}

		err = consumer.SubscribeTopics([]string{kcs.Topic}, nil)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}

		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				panic(err)
			}

			kafkaCallFnWithResilence(msg, kc, *kcs, fn)
			consumer.CommitMessage(msg)
		}

	}(topic)
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
