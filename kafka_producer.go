package goframework

import (
	"context"
	"encoding/json"
	"os"

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

func NewKafkaProducer[T interface{}](k *GoKafka,
	kcs *KafkaProducerSettings) Producer[T] {

	hostname, _ := os.Hostname()

	kcm := &kafka.ConfigMap{
		"bootstrap.servers": k.server,
		"client.id":         hostname,
		"acks":              "all"}

	// CreateKafkaTopic(context.Background(), kcm, &TopicConfiguration{
	// 	Topic:             kcs.Topic,
	// 	NumPartitions:     kcs.NumPartitions,
	// 	ReplicationFactor: kcs.ReplicationFactor,
	// })

	return &KafkaProducer[T]{
		kcs: kcs,
		kcm: kcm,
	}
}

func (kp *KafkaProducer[T]) Publish(ctx context.Context, msgs ...*T) error {
	p, err := kafka.NewProducer(kp.kcm)
	if err != nil {
		return err
	}
	defer p.Close()

	for _, m := range msgs {

		headers := helperContextKafka(ctx,
			map[string]string{
				"X-Tenant-Id":      "X-Tenant-Id",
				"X-Author":         "X-Author",
				"X-Correlation-Id": "X-Correlation-Id"})

		hasCorrelationID := false
		for _, v := range headers {
			if v.Key == "X-Correlation-Id" && len(v.Value) > 0 {
				hasCorrelationID = true
			}
		}
		if !hasCorrelationID {
			headers = append(headers, kafka.Header{Key: "X-Correlation-Id", Value: []byte(uuid.NewString())})
		}

		data, err := json.Marshal(m)
		if err != nil {
			return err
		}

		if err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{
			Topic:     &kp.kcs.Topic,
			Partition: kp.kcs.Partition,
			Offset:    kp.kcs.Offset,
		}, Value: data, Headers: headers}, nil); err != nil {
			return err
		}

		p.Flush(kp.kcs.TimeoutFlush)
	}

	return nil
}
