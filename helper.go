package goframework

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	XTENANTID      string = "X-Tenant-Id"
	TTENANTID      string = "tenant_id"
	XAUTHOR        string = "X-Author"
	XAUTHORID      string = "X-Author-Id"
	XCORRELATIONID string = "X-Correlation-Id"
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

func GetContextHeader(c context.Context, keys ...string) string {
	for _, key := range keys {
		switch c := c.(type) {
		case *gin.Context:
			if tid, err := GetTenantByToken(c); err == nil {
				return tid.String()
			}
			if sid := c.Request.Header.Get(key); sid != "" {
				return sid
			}
		case *ConsumerContext:
			for _, kh := range c.Msg.Headers {
				if kh.Key == key && len(kh.Value) > 0 {
					return string(kh.Value)
				}
			}
		default:
			return fmt.Sprint(c.Value(key))
		}
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

func ToContext(c context.Context) context.Context {
	listContext := []string{XTENANTID, XAUTHOR, XAUTHORID, XCORRELATIONID, TTENANTID}

	cc := context.Background()
	switch c := c.(type) {
	case *gin.Context:
		for _, v := range listContext {
			cc = context.WithValue(cc, v, c.Request.Header.Get(v))
		}
	case *ConsumerContext:
		for _, v := range listContext {
			for _, kh := range c.Msg.Headers {
				if kh.Key == v {
					cc = context.WithValue(cc, v, string(kh.Value))
					break
				}
			}
		}
	default:
		for _, v := range listContext {
			cc = context.WithValue(cc, v, fmt.Sprint(c.Value(v)))
		}
	}
	return cc
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

func GetTenantByToken(ctx *gin.Context) (uuid.UUID, error) {
	tokenString := ctx.GetHeader("Authorization")

	tokenString = strings.Replace(tokenString, "Bearer ", "", 1)
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return uuid.Nil, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		tenant := fmt.Sprint(claims[TTENANTID])
		if tenant == "" {
			return uuid.Nil, fmt.Errorf("Tenant not found")
		}
		id, err := uuid.Parse(tenant)
		if err != nil {
			return uuid.Nil, fmt.Errorf("Tenant not found")
		}

		return id, nil
	} else {
		return uuid.Nil, fmt.Errorf("Tenant not found")
	}
}
