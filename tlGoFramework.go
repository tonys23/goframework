package goframework

import (
	"crypto/tls"
	"log"
	"os"
	"strconv"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/dig"
)

type GoFramework struct {
	ioc           *dig.Container
	configuration *viper.Viper
	server        *gin.Engine
	nrApplication gfAgentTelemetry
}

type GoFrameworkOptions interface {
	run(gf *GoFramework)
}

func NewGoFramework(opts ...GoFrameworkOptions) *GoFramework {

	gf := &GoFramework{
		ioc:           dig.New(),
		configuration: initializeViper(),
		server:        gin.Default(),
	}

	gf.server.Use(cors.Default())

	for _, opt := range opts {
		opt.run(gf)
	}

	gf.ioc.Provide(initializeViper)
	gf.ioc.Provide(newLog)
	gf.ioc.Provide(func() gfAgentTelemetry { return gf.nrApplication })

	if gf.nrApplication != nil {
		gf.server.Use(gf.nrApplication.gin())
	}
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
func (gf *GoFramework) RegisterRepository(constructor interface{}) {
	err := gf.ioc.Provide(constructor)
	if err != nil {
		log.Fatalln(err)
	}
}

func (gf *GoFramework) RegisterApplication(application interface{}) {
	err := gf.ioc.Provide(application)
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
	port := os.Getenv("port")
	if port == "" {
		port = "8081"
	}
	return gf.server.Run(":" + port)
}

// mongo
func (gf *GoFramework) RegisterDbMongo(host string, user string, pass string, database string) {

	opts := options.Client().ApplyURI(host)

	if user != "" {
		opts.SetAuth(options.Credential{Username: user, Password: pass})
	}

	if gf.nrApplication != nil {
		opts = opts.SetMonitor(gf.nrApplication.mongoMonitor())
	}

	err := gf.ioc.Provide(func() *mongo.Database { return (newMongoClient(opts).Database(database)) })
	if err != nil {
		log.Fatalln(err)
	}
}

// Redis
func (gf *GoFramework) RegisterRedis(address string, password string, db string) {

	dbInt, err := strconv.Atoi(db)
	if err != nil {
		log.Fatalln(err)
	}

	opts := &redis.Options{
		Addr:     address,
		Password: password,
		DB:       dbInt,
	}

	if opts.Addr != "" && opts.Addr != "localhost:6379" {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	err = gf.ioc.Provide(func() *redis.Client { return (newRedisClient(opts)) })
	if err != nil {
		log.Fatalln(err)
	}

}

func (gf *GoFramework) RegisterCache(constructor interface{}) {
	err := gf.ioc.Provide(constructor)
	if err != nil {
		log.Fatalln(err)
	}
}

func (gf *GoFramework) RegisterKafka(server string, groupId string) {
	err := gf.ioc.Provide(func() *GoKafka {
		kc := NewKafkaConfigMap(server, groupId)
		if gf.nrApplication != nil {
			kc.newMonitor(gf.nrApplication.getAgent())
		}
		return kc
	})
	if err != nil {
		log.Fatalln(err)
	}
}

// Kafka
func (gf *GoFramework) RegisterKafkaProducer(producer interface{}) {
	err := gf.ioc.Provide(producer)
	if err != nil {
		log.Fatalln(err)
	}
}

func (gf *GoFramework) RegisterKafkaConsumer(consumer interface{}) {
	err := gf.ioc.Invoke(consumer)
	if err != nil {
		log.Fatalln(err)
	}
}
