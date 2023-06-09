package goframework

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
)

func helperContext(c context.Context, filter map[string]interface{}, addfilter map[string]string) {
	switch c := c.(type) {
	case *gin.Context:
		for k, v := range addfilter {
			value := string(c.Request.Header.Get(v))
			if value != "" {
				filter[k] = value
			}
		}
	case *ConsumerContext:
		for k, v := range addfilter {
			for _, kh := range c.Msg.Headers {
				if kh.Key == v {
					filter[k] = string(kh.Value)
					break
				}
			}
		}
	default:
		for k, v := range addfilter {
			value := fmt.Sprint(c.Value(v))
			if value != "" {
				filter[k] = value
				break
			}
		}
	}
}

func getContextHeader(c context.Context, key string) string {

	switch c := c.(type) {
	case *gin.Context:
		return c.Request.Header.Get(key)
	case *ConsumerContext:
		for _, kh := range c.Msg.Headers {
			if kh.Key == key {
				return string(kh.Value)
			}
		}
	default:
		return fmt.Sprint(c.Value(key))
	}
	return ""
}

func getContext(c context.Context) context.Context {

	switch c := c.(type) {
	case *gin.Context:
		return c.Request.Context()
	default:
		return c
	}
}

func helperContextKafka(c context.Context, addfilter map[string]string) []kafka.Header {

	var filter []kafka.Header
	switch c := c.(type) {
	case *gin.Context:
		for k, v := range addfilter {
			filter = append(filter, kafka.Header{Key: k, Value: []byte(c.Request.Header.Get(v))})
		}
	case *ConsumerContext:
		for k, v := range addfilter {
			for _, kh := range c.Msg.Headers {
				if kh.Key == v {
					filter = append(filter, kafka.Header{Key: k, Value: []byte(kh.Value)})
					break
				}
			}
		}
	default:
		for k, v := range addfilter {
			filter = append(filter, kafka.Header{Key: k, Value: []byte(fmt.Sprint(c.Value(v)))})
		}
	}
	return filter
}

func structToBson(inputStruct interface{}) bson.M {
	inputType := reflect.TypeOf(inputStruct)
	inputValue := reflect.ValueOf(inputStruct)

	output := bson.M{}

	for i := 0; i < inputType.NumField(); i++ {
		field := inputType.Field(i)
		value := inputValue.Field(i)

		if !reflect.DeepEqual(value.Interface(), reflect.Zero(field.Type).Interface()) {
			output[strings.ToLower(field.Name)] = value.Interface()
		}
	}

	return output
}
