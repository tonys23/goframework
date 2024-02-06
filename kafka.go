package goframework

import (
	"context"
	"errors"
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
		server           string
		groupId          string
		nrapp            *newrelic.Application
		securityprotocol string
		saslmechanism    string
		saslusername     string
		saslpassword     string
	}
)

func NewKafkaConfigMap(connectionString string,
	groupId string,
	securityprotocol string,
	saslmechanism string,
	saslusername string,
	saslpassword string) *GoKafka {
	return &GoKafka{
		server:           connectionString,
		groupId:          groupId,
		securityprotocol: securityprotocol,
		saslmechanism:    saslmechanism,
		saslusername:     saslusername,
		saslpassword:     saslpassword,
	}
}

func (k *GoKafka) newMonitor(nrapp *newrelic.Application) {
	k.nrapp = nrapp
}

func wait_until(fn func() bool) {
	for fn() {
		time.Sleep(time.Second)
	}
}

func recover_all() {
	if e := recover(); e != nil {
		switch ee := e.(type) {
		case error:
			fmt.Println(ee)
		case string:
			fmt.Println(errors.New(ee))
		default:
			fmt.Println(fmt.Errorf("undefined error: %v", ee))
		}
	}
}

type (
	ConsumerMultiRoutineSettings struct {
		Routines          int
		AutoOffsetReset   string
		Numpartitions     int
		Retries           int
		ReplicationFactor int
	}
)

func (k *GoKafka) ConsumerMultiRoutine(
	topic string,
	fn ConsumerFunc,
	cfg ConsumerMultiRoutineSettings) {
	go func(topic string) {

		kcs := &KafkaConsumerSettings{
			Topic:           topic,
			AutoOffsetReset: cfg.AutoOffsetReset,
			Retries:         uint16(cfg.Retries),
		}

		kc := &kafka.ConfigMap{
			"bootstrap.servers":             k.server,
			"group.id":                      k.groupId,
			"auto.offset.reset":             kcs.AutoOffsetReset,
			"partition.assignment.strategy": "cooperative-sticky",
			"enable.auto.commit":            false,
		}

		if len(k.securityprotocol) > 0 {
			kc.SetKey("security.protocol", k.securityprotocol)
		}

		if len(k.saslmechanism) > 0 {
			kc.SetKey("sasl.mechanism", k.saslmechanism)
		}

		if len(k.saslusername) > 0 {
			kc.SetKey("sasl.username", k.saslusername)
		}

		if len(k.saslpassword) > 0 {
			kc.SetKey("sasl.password", k.saslpassword)
		}

		fmt.Fprintf(os.Stdout,
			"%% Start consumer %s \n",
			k.groupId)

		CreateKafkaTopic(context.Background(), kc, &TopicConfiguration{
			Topic:             topic,
			NumPartitions:     cfg.Numpartitions,
			ReplicationFactor: cfg.ReplicationFactor,
		})

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
		r := 0
		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				log.Println(err.Error())
				continue
			}
			r++
			go func(cmsg *kafka.Message,
				cconsumer *kafka.Consumer,
				ckc *kafka.ConfigMap,
				ckcs KafkaConsumerSettings,
				nrapp *newrelic.Application,
				cfn ConsumerFunc) {
				defer recover_all()
				defer cconsumer.CommitMessage(cmsg)
				defer func() {
					r--
				}()

				ctx := context.Background()
				transaction := &newrelic.Transaction{}
				if nrapp != nil {
					transaction = k.nrapp.StartTransaction("kafka/consumer")
					ctx = newrelic.NewContext(ctx, transaction)
				}
				kafkaCallFnWithResilence(ctx, cmsg, ckc, ckcs, cfn)
				if nrapp != nil {
					transaction.End()
				}
			}(msg, consumer, kc, *kcs, k.nrapp, fn)
			wait_until(func() bool {
				return r >= cfg.Routines
			})
		}
	}(topic)
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

		if len(k.securityprotocol) > 0 {
			kc.SetKey("security.protocol", k.securityprotocol)
		}

		if len(k.saslmechanism) > 0 {
			kc.SetKey("sasl.mechanism", k.saslmechanism)
		}

		if len(k.saslusername) > 0 {
			kc.SetKey("sasl.username", k.saslusername)
		}

		if len(k.saslpassword) > 0 {
			kc.SetKey("sasl.password", k.saslpassword)
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
				log.Println(err.Error())
				continue
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
