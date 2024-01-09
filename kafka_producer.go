package goframework

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/newrelic/go-agent/v3/newrelic"
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
		kp  *kafka.Producer
	}
)

func NewKafkaProducer[T interface{}](k *GoKafka,
	kcs *KafkaProducerSettings) Producer[T] {

	hostname, _ := os.Hostname()

	kcm := &kafka.ConfigMap{
		"bootstrap.servers": k.server,
		"client.id":         hostname,
		"acks":              "all",
	}

	if len(k.securityprotocol) > 0 {
		kcm.SetKey("security.protocol", k.securityprotocol)
	}

	if len(k.saslmechanism) > 0 {
		kcm.SetKey("sasl.mechanism", k.saslmechanism)
	}

	if len(k.saslusername) > 0 {
		kcm.SetKey("sasl.username", k.saslusername)
	}

	if len(k.saslpassword) > 0 {
		kcm.SetKey("sasl.password", k.saslpassword)
	}

	// CreateKafkaTopic(context.Background(), kcm, &TopicConfiguration{
	// 	Topic:             kcs.Topic,
	// 	NumPartitions:     kcs.NumPartitions,
	// 	ReplicationFactor: kcs.ReplicationFactor,
	// })
	kp, err := kafka.NewProducer(kcm)
	if err != nil {
		return nil
	}

	// go func() {
	// 	for e := range kp.Events() {
	// 		switch ev := e.(type) {
	// 		case *kafka.Message:
	// 			if ev.TopicPartition.Error != nil {
	// 				fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
	// 			} else {
	// 				fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
	// 					*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
	// 			}
	// 		}
	// 	}
	// }()

	return &KafkaProducer[T]{
		kcs: kcs,
		kcm: kcm,
		k:   k,
		kp:  kp,
	}
}

func (kp *KafkaProducer[T]) Publish(ctx context.Context, msgs ...*T) error {

	txn := newrelic.FromContext(ctx)
	nrSegment := txn.StartSegment(kp.kcs.Topic)
	nrSegment.AddAttribute("span.kind", "client")
	defer nrSegment.End()

	headers := helperContextKafka(ctx,
		map[string]string{
			"X-Tenant-Id":      "X-Tenant-Id",
			"X-Author":         "X-Author",
			"X-Author-Id":      "X-Author-Id",
			"X-Correlation-Id": "X-Correlation-Id"})

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
		if err = kp.kp.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{
			Topic:     &kp.kcs.Topic,
			Partition: kafka.PartitionAny,
			Offset:    kp.kcs.Offset,
		}, Value: data, Headers: headers}, delivery_chan); err != nil {
			fmt.Println(err.Error())
			return err
		}
	}

	return nil
}
