package goframework

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type (
	KafkaConsumerSettings struct {
		Topic             string
		NumPartitions     int
		ReplicationFactor int
		GroupId           string
		AutoOffsetReset   string
		Retries           uint16
	}

	KafkaConsumer struct {
		kcs *KafkaConsumerSettings
		kcm *kafka.ConfigMap
		fn  ConsumerFunc
	}

	KafkaContext struct {
		Msg              *kafka.Message
		RemainingRetries int
		Faulted          bool
	}
)

func NewKafkaConsumer(kcm *kafka.ConfigMap,
	kcs *KafkaConsumerSettings, fn ConsumerFunc) Consumer {

	CreateKafkaTopic(context.Background(), kcm, &TopicConfiguration{
		Topic:             kcs.Topic,
		NumPartitions:     kcs.NumPartitions,
		ReplicationFactor: kcs.ReplicationFactor,
	})

	cmt := *kcm
	cmt.SetKey("group.id", kcs.GroupId)
	cmt.SetKey("auto.offset.reset", kcs.AutoOffsetReset)
	kc := &KafkaConsumer{
		kcs: kcs,
		kcm: &cmt,
	}
	return kc
}

func (kc *KafkaConsumer) HandleFn() {
	c, err := kafka.NewConsumer(kc.kcm)
	if err != nil {
		panic(err)
	}

	defer c.Close()
	defer func() {
		if e := recover(); e != nil {
			kc.HandleFn()
		}
	}()
	c.SubscribeTopics([]string{kc.kcs.Topic}, nil)
	for {

		msg, err := c.ReadMessage(-1)
		if err != nil {
			panic(err)
		}

		kafkaCallFnWithResilence(msg, kc.kcm, *kc.kcs, kc.fn)
		c.CommitMessage(msg)
	}
}

func kafkaCallFnWithResilence(msg *kafka.Message,
	kcm *kafka.ConfigMap,
	kcs KafkaConsumerSettings,
	fn ConsumerFunc) {

	cctx := &ConsumerContext{
		Context:          context.Background(),
		RemainingRetries: kcs.Retries,
		Faulted:          kcs.Retries == 0,
		Msg:              msg}

	defer func() {
		if e := recover(); e != nil {
			err := e.(error)
			fmt.Println(err.Error())
			if kcs.Retries > 1 {
				kcs.Retries--
				kafkaCallFnWithResilence(msg, kcm, kcs, fn)
				return
			}

			kafkaSendToDlq(&kcs, kcm, msg)
		}
	}()

	fn(cctx)
}

func kafkaSendToDlq(
	kcs *KafkaConsumerSettings,
	kcm *kafka.ConfigMap,
	msg *kafka.Message) {
	p, err := kafka.NewProducer(kcm)

	if err != nil {
		panic(err)
	}

	defer p.Close()

	tpn := *msg.TopicPartition.Topic + "_error"

	CreateKafkaTopic(context.Background(), kcm, &TopicConfiguration{
		Topic:             tpn,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})

	msg.TopicPartition.Topic = &tpn
	msg.TopicPartition.Partition = kafka.PartitionAny
	if err = p.Produce(msg, nil); err != nil {
		panic(err)
	}

	p.Flush(15 * 100)
}
