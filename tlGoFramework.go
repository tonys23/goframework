package goframework

import (
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"go.uber.org/dig"
)

type GoFramework struct {
	ioc    *dig.Container
	server *gin.Engine
}

func NewGoFramework() *GoFramework {
	gf := &GoFramework{
		ioc:    dig.New(),
		server: gin.Default(),
	}

	gf.ioc.Provide(initializeViper)
	err := gf.ioc.Provide(func() *gin.Engine { return gf.server })
	if err != nil {
		log.Panic(err)
	}

	return gf
}

// VIPER
func initializeViper() *viper.Viper {
	v := viper.New()
	v.AddConfigPath("../configs")
	v.SetConfigType("json")
	v.SetConfigName(os.Getenv("env"))
	if err := v.ReadInConfig(); err != nil {
		panic(err)
	}
	return v
}

// DIG
func (gf *GoFramework) AddProvide(constructor interface{}) error {
	err := gf.ioc.Provide(constructor)
	return err
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
