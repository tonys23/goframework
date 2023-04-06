package goframework

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
)

func helperContext(c context.Context, filter map[string]interface{}, addfilter map[string]string) {
	switch c := c.(type) {
	case *gin.Context:
		for k, v := range addfilter {
			value := string(c.Request.Header.Get(v))
			if value != "" {
				filter[k] = string(c.Request.Header.Get(v))
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
		fmt.Println("KFK")
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
		fmt.Println("KFK")
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
		fmt.Println("KFK")
	}
	return filter
}
