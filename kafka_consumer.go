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
	}

	KafkaContext struct {
		Msg              *kafka.Message
		RemainingRetries int
		Faulted          bool
	}
)

func NewKafkaConsumer(kcm *kafka.ConfigMap,
	kcs *KafkaConsumerSettings, fn ConsumerFunc) *KafkaConsumer {

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

func kafkaCallFnWithResilence(
	ctx context.Context,
	tm *TracingMonitor,
	msg *kafka.Message,
	kcm *kafka.ConfigMap,
	kcs KafkaConsumerSettings,
	fn ConsumerFunc) {

	cctx := &ConsumerContext{
		Context:          ctx,
		RemainingRetries: kcs.Retries,
		Faulted:          kcs.Retries == 0,
		Msg:              msg}

	defer func() {
		if e := recover(); e != nil {
			err := e.(error)
			fmt.Println(err.Error())
			if kcs.Retries > 1 {
				kcs.Retries--
				tm.AddStack(500, fmt.Sprintf("CONSUMER ERROR. RETRY %d: %s", kcs.Retries, err.Error()))
				kafkaCallFnWithResilence(ctx, tm, msg, kcm, kcs, fn)
				return
			}
			kafkaSendToDlq(cctx, tm, &kcs, kcm, msg, err)
			tm.AddStack(500, "CONSUMER ERROR.")

		}
	}()

	tm.AddStack(100, "CONSUMING...")
	fn(cctx)
	tm.AddStack(200, "SUCCESSFULLY CONSUMED")
}

func kafkaSendToDlq(
	ctx context.Context,
	tm *TracingMonitor,
	kcs *KafkaConsumerSettings,
	kcm *kafka.ConfigMap,
	msg *kafka.Message,
	er error) {
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
	msg.Key = []byte(er.Error())
	dlc := make(chan kafka.Event)
	if er = p.Produce(msg, dlc); er != nil {
		tm.AddStack(500, "ERROR TO PRODUCE ERROR MSG: "+er.Error())
		panic(er)
	}
	<-dlc
	tm.AddStack(100, "ERROR PRODUCED IN "+tpn)
}
