package goframework

import (
	"context"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
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

	ctx := context.Background()
	exp, err := newExporter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(newResource()),
	)
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()
	otel.SetTracerProvider(tp)

	_, span := otel.Tracer("teste").Start(ctx, "Run")
	defer span.End()

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

func (gf *GoFramework) RegisterKafka(server string, groupId string) {
	err := gf.ioc.Provide(func() *GoKafka { return NewKafkaConfigMap(server, groupId) })
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
