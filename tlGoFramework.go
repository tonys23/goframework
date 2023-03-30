package goframework

import (
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/dig"
	"go.uber.org/zap"
)

type GoFramework struct {
	ioc           *dig.Container
	configuration *viper.Viper
	server        *gin.Engine
	projectName   string
	traceProvider *sdktrace.TracerProvider
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

	for _, opt := range opts {
		opt.run(gf)
	}

	otel.SetTracerProvider(gf.traceProvider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	gf.ioc.Provide(initializeLog)
	gf.ioc.Provide(initializeViper)
	gf.server.Use(otelgin.Middleware(gf.projectName, otelgin.WithTracerProvider(gf.traceProvider)))
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

func initializeLog() *GfLogger {
	logger, _ := zap.NewProduction()
	return &GfLogger{logger}
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
	return gf.server.Run(":8081")
}

// mongo
func (gf *GoFramework) RegisterDbMongo(host string, user string, pass string, database string) {

	opts := options.Client().ApplyURI(host).SetAuth(options.Credential{Username: user, Password: pass})
	opts.Monitor = otelmongo.NewMonitor(otelmongo.WithTracerProvider(gf.traceProvider))

	err := gf.ioc.Provide(func() *mongo.Database { return (newMongoClient(opts).Database(database)) })
	if err != nil {
		log.Fatalln(err)
	}
}

func (gf *GoFramework) RegisterKafka(server string, groupId string) {
	err := gf.ioc.Provide(func() *GoKafka {
		kc := NewKafkaConfigMap(server, groupId)
		kc.newMonitor(gf.traceProvider)
		return kc
	})
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

func (gf *GoFramework) RegisterKafkaConsumer(consumer interface{}) {
	err := gf.ioc.Invoke(consumer)
	if err != nil {
		log.Fatalln(err)
	}
}
