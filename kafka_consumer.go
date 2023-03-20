package goframework

import (
	"context"
	"encoding/json"
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

	KafkaConsumer[T interface{}] struct {
		kcs *KafkaConsumerSettings
		kcm *kafka.ConfigMap
	}
)

func NewKafkaConsumer[T interface{}](kcm *kafka.ConfigMap,
	kcs *KafkaConsumerSettings) Consumer[T] {

	CreateKafkaTopic(context.Background(), kcm, &TopicConfiguration{
		Topic:             kcs.Topic,
		NumPartitions:     kcs.NumPartitions,
		ReplicationFactor: kcs.ReplicationFactor,
	})

	cmt := *kcm
	cmt.SetKey("group.id", kcs.GroupId)
	cmt.SetKey("auto.offset.reset", kcs.AutoOffsetReset)
	kc := &KafkaConsumer[T]{
		kcs: kcs,
		kcm: &cmt,
	}
	return kc
}

func (kc *KafkaConsumer[T]) HandleFn(fn ConsumerFunc[T]) {
	c, err := kafka.NewConsumer(kc.kcm)
	if err != nil {
		panic(err)
	}

	defer c.Close()
	defer func() {
		if e := recover(); e != nil {
			kc.HandleFn(fn)
		}
	}()
	c.SubscribeTopics([]string{kc.kcs.Topic}, nil)
	for {
		msg, err := c.ReadMessage(-1)
		if err != nil {
			panic(err)
		}

		kafkaCallFnWithResilence(msg, kc.kcm, *kc.kcs, fn)
		c.CommitMessage(msg)
	}
}

func kafkaCallFnWithResilence[T interface{}](msg *kafka.Message,
	kcm *kafka.ConfigMap,
	kcs KafkaConsumerSettings,
	fn ConsumerFunc[T]) {

	ctx := context.Background()
	cctx := ConsumerContext{RemainingRetries: kcs.Retries, Faulted: kcs.Retries == 0}
	var payload T
	err := json.Unmarshal(msg.Value, &payload)
	if err != nil {
		kafkaSendToDlq(ctx, &kcs, kcm, msg)
		return
	}

	defer func() {
		if e := recover(); e != nil {
			err = e.(error)
			fmt.Println(err.Error())
			if kcs.Retries > 1 {
				kcs.Retries--
				kafkaCallFnWithResilence(msg, kcm, kcs, fn)
				return
			}

			kafkaSendToDlq(ctx, &kcs, kcm, msg)
		}
	}()

	fn(ctx, cctx, payload)
}

func kafkaSendToDlq(
	ctx context.Context,
	kcs *KafkaConsumerSettings,
	kcm *kafka.ConfigMap,
	msg *kafka.Message) {
	p, err := kafka.NewProducer(kcm)

	if err != nil {
		panic(err)
	}

	defer p.Close()

	tpn := *msg.TopicPartition.Topic + "_error"

	CreateKafkaTopic(ctx, kcm, &TopicConfiguration{
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
