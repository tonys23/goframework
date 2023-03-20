package goframework

import (
	"context"
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

type (
	KafkaProducerSettings struct {
		Topic             string
		NumPartitions     int
		ReplicationFactor int

		Partition    int32
		Offset       kafka.Offset
		TimeoutFlush int
	}

	KafkaProducer[T interface{}] struct {
		kcs *KafkaProducerSettings
		kcm *kafka.ConfigMap
	}
)

func NewKafkaProducer[T interface{}](kcm *kafka.ConfigMap,
	kcs *KafkaProducerSettings) Producer[T] {

	CreateKafkaTopic(context.Background(), kcm, &TopicConfiguration{
		Topic:             kcs.Topic,
		NumPartitions:     kcs.NumPartitions,
		ReplicationFactor: kcs.ReplicationFactor,
	})

	return &KafkaProducer[T]{
		kcs: kcs,
		kcm: kcm,
	}
}

func (kp *KafkaProducer[T]) Publish(correlationId uuid.UUID, msgs ...*T) error {
	p, err := kafka.NewProducer(kp.kcm)
	if err != nil {
		return err
	}
	defer p.Close()

	for _, m := range msgs {

		data, err := json.Marshal(m)
		if err != nil {
			return err
		}

		if err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{
			Topic:     &kp.kcs.Topic,
			Partition: kp.kcs.Partition,
			Offset:    kp.kcs.Offset,
		}, Value: data}, nil); err != nil {
			return err
		}

		p.Flush(kp.kcs.TimeoutFlush)
	}

	return nil
}
