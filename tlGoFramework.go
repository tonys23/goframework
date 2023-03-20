package goframework

import (
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/dig"
)

type GoFramework struct {
	ioc           *dig.Container
	configuration *viper.Viper
	server        *gin.Engine
}

func NewGoFramework() *GoFramework {
	gf := &GoFramework{
		ioc:           dig.New(),
		configuration: initializeViper(),
		server:        gin.Default(),
	}
	gf.ioc.Provide(initializeViper)
	// err := gf.ioc.Provide(func() *gin.Engine { return gf.server })
	err := gf.ioc.Provide(func() *gin.RouterGroup { return gf.server.Group("/") })
	if err != nil {
		log.Panic(err)
	}

	return gf
}

// VIPER
func initializeViper() *viper.Viper {
	v := viper.New()
	v.AddConfigPath("./configs")
	v.SetConfigType("json")
	v.SetConfigName(os.Getenv("env"))
	if err := v.ReadInConfig(); err != nil {
		panic(err)
	}
	return v
}

func (gf *GoFramework) GetConfig(key string) string {
	return gf.configuration.GetString(key)
}

// DIG
func (gf *GoFramework) Register(constructor interface{}) {
	err := gf.ioc.Provide(constructor)
	if err != nil {
		log.Fatalln(err)
	}
}

// GIN
func (gf *GoFramework) RegisterController(controller interface{}) {
	err := gf.ioc.Invoke(controller)
	if err != nil {
		log.Fatalln(err)
	}
}

func (gf *GoFramework) Start() error {
	return gf.server.Run(":8081")
}

// mongo
func (gf *GoFramework) RegisterDbMongo(host string, user string, pass string, database string) {
	opts := options.Client().ApplyURI(host).SetAuth(options.Credential{Username: user, Password: pass})
	err := gf.ioc.Provide(func() *mongo.Database { return (newMongoClient(opts).Database(database)) })
	if err != nil {
		log.Fatalln(err)
	}
}

func (gf *GoFramework) RegisterKafka(server string) {
	err := gf.ioc.Provide(func() *kafka.ConfigMap { return NewKafkaConfigMap(server) })
	if err != nil {
		log.Fatalln(err)
	}
}

func (gf *GoFramework) RegisterKafkaProducer(producer interface{}) {
	err := gf.ioc.Provide(producer)
	if err != nil {
		log.Fatalln(err)
	}
}

func (gf *GoFramework) RegisterKafkaConsumer(consumer Consumer[interface{}], h ConsumerFunc[interface{}]) {
	err := gf.ioc.Provide(consumer)
	if err != nil {
		log.Fatalln(err)
		return
	}

	go consumer.HandleFn(h)
}
