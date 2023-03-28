package goframework

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
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
		k   *GoKafka
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
		k:   k,
	}
}

func (kp *KafkaProducer[T]) Publish(ctx context.Context, msgs ...*T) error {

	headers := helperContextKafka(ctx,
		map[string]string{
			"X-Tenant-Id":      "X-Tenant-Id",
			"X-Author":         "X-Author",
			"X-Correlation-Id": "X-Correlation-Id"})

	tracer := kp.k.traceProvider.Tracer(
		"Kafka_Consumer",
	)

	_, span := tracer.Start(getContext(ctx),
		kp.kcs.Topic,
		trace.WithAttributes(semconv.MessagingSystem("kafka")),
		trace.WithAttributes(semconv.MessagingDestinationName(kp.kcs.Topic)),
		trace.WithSpanKind(trace.SpanKindProducer),
	)

	defer span.End()

	p, err := kafka.NewProducer(kp.kcm)
	if err != nil {
		return err
	}
	defer p.Close()

	for _, m := range msgs {

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

		delivery_chan := make(chan kafka.Event, 10000)
		if err = p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{
			Topic:     &kp.kcs.Topic,
			Partition: kafka.PartitionAny,
			Offset:    kp.kcs.Offset,
		}, Value: data, Headers: headers}, delivery_chan); err != nil {
			return err
		}

		go func() {
			for e := range p.Events() {
				switch ev := e.(type) {
				case *kafka.Message:
					if ev.TopicPartition.Error != nil {
						log.Fatalf("Failed to deliver message: %v\n", ev.TopicPartition)
					} else {
						log.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
							*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
					}
				}
			}
		}()
	}

	return nil
}
