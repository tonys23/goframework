package goframework

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	newrelic "github.com/newrelic/go-agent/v3/newrelic"
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
		nrapp   *newrelic.Application
	}
)

func NewKafkaConfigMap(connectionString string, groupId string) *GoKafka {
	return &GoKafka{
		server:  connectionString,
		groupId: groupId,
	}
}

func (k *GoKafka) newMonitor(nrapp *newrelic.Application) {
	k.nrapp = nrapp
}

func (k *GoKafka) Consumer(topic string, fn ConsumerFunc) {
	go func(topic string) {

		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: "earliest",
			Retries:         5,
		}

		kc := &kafka.ConfigMap{
			"bootstrap.servers":             k.server,
			"group.id":                      k.groupId,
			"auto.offset.reset":             kcs.AutoOffsetReset,
			"partition.assignment.strategy": "cooperative-sticky",
			"enable.auto.commit":            false,
		}

		fmt.Fprintf(os.Stdout,
			"%% Start consumer %s \n",
			k.groupId)

		consumer, err := kafka.NewConsumer(kc)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}

		err = consumer.SubscribeTopics([]string{kcs.Topic}, rebalanceCallback)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}

		for {
			msg, err := consumer.ReadMessage(-1)

			ctx := context.Background()
			transaction := &newrelic.Transaction{}
			if k.nrapp != nil {
				transaction = k.nrapp.StartTransaction("kafka/consumer")
				ctx = newrelic.NewContext(ctx, transaction)
			}

			if err != nil {
				panic(err)
			}

			kafkaCallFnWithResilence(ctx, msg, kc, *kcs, fn)
			consumer.CommitMessage(msg)

			if k.nrapp != nil {
				transaction.End()
			}
		}

	}(topic)
}

func rebalanceCallback(c *kafka.Consumer, event kafka.Event) error {

	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		fmt.Fprintf(os.Stderr,
			"%% %s rebalance: %d new partition(s) assigned: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions),
			ev.Partitions)

		err := c.IncrementalAssign(ev.Partitions)
		if err != nil {
			panic(err)
		}

	case kafka.RevokedPartitions:
		fmt.Fprintf(os.Stderr,
			"%% %s rebalance: %d partition(s) revoked: %v\n",
			c.GetRebalanceProtocol(), len(ev.Partitions),
			ev.Partitions)
		if c.AssignmentLost() {
			fmt.Fprintf(os.Stderr, "%% Current assignment lost!\n")
		}
	}

	return nil
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
