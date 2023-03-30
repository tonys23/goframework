package goframework

import (
	"context"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

type (
	TopicConfiguration struct {
		Topic             string
		NumPartitions     int
		ReplicationFactor int
	}

	GoKafka struct {
		server        string
		groupId       string
		traceProvider *sdktrace.TracerProvider
	}
)

func NewKafkaConfigMap(connectionString string, groupId string) *GoKafka {
	return &GoKafka{
		server:  connectionString,
		groupId: groupId,
	}
}

func (k *GoKafka) newMonitor(tp *sdktrace.TracerProvider) {
	k.traceProvider = tp
}

func (k *GoKafka) Consumer(topic string, fn ConsumerFunc) {
	go func(topic string) {

		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: "earliest",
			Retries:         5,
		}

		tracer := k.traceProvider.Tracer(
			"kafka-consumer",
		)

		kc := &kafka.ConfigMap{
			"bootstrap.servers": k.server,
			"group.id":          k.groupId,
			"auto.offset.reset": kcs.AutoOffsetReset}

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

			ctx := context.Background()
			ctx, span := tracer.Start(ctx, "consumer:"+topic,
				trace.WithAttributes(semconv.MessagingSystem("kafka")),
				trace.WithAttributes(semconv.MessagingDestinationName(topic)),
				trace.WithSpanKind(trace.SpanKindProducer))

			if err != nil {
				panic(err)
			}

			kafkaCallFnWithResilence(ctx, msg, kc, *kcs, fn)
			consumer.CommitMessage(msg)

			span.End()
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
