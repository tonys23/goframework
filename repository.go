package goframework

import (
	"context"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDbRepository[T interface{}] struct {
	collection *mongo.Collection
}

func NewMongoDbRepository[T interface{}](
	db *mongo.Database) IRepository[T] {
	var r T
	coll := db.Collection(strings.ToLower(reflect.TypeOf(r).Name()))
	return &MongoDbRepository[T]{
		collection: coll,
	}
}

func (r *MongoDbRepository[T]) GetAll(
	ctx context.Context,
	filter map[string]interface{}) *[]T {

	helperContext(ctx, filter, map[string]string{"tenantId": "X-Tenant-Id"})
	cur, err := r.collection.Find(ctx, filter)
	if err != nil {
		panic(err)
	}
	result := []T{}
	for cur.Next(ctx) {
		var el T
		err = cur.Decode(&el)
		if err != nil {
			panic(err)
		}
		result = append(result, el)
	}

	return &result
}

func (r *MongoDbRepository[T]) GetAllSkipTake(
	ctx context.Context,
	filter map[string]interface{},
	skip int64,
	take int64) *[]T {

	helperContext(ctx, filter, map[string]string{"tenantId": "X-Tenant-Id"})
	op := options.Find()
	op.SetSkip(skip)
	op.SetLimit(take)
	cur, err := r.collection.Find(ctx, filter, op)

	if err != nil {
		panic(err)
	}
	result := []T{}
	for cur.Next(ctx) {
		var el T
		err = cur.Decode(&el)
		if err != nil {
			panic(err)
		}
		result = append(result, el)
	}

	return &result
}

func (r *MongoDbRepository[T]) GetFirst(
	ctx context.Context,
	filter map[string]interface{}) *T {
	var el T

	helperContext(ctx, filter, map[string]string{"tenantId": "X-Tenant-Id"})

	// filter["tenantId"] = ctx.(*gin.Context).Request.Header.Get("X-Tenant-Id")

	err := r.collection.FindOne(ctx, filter).Decode(&el)

	if err == mongo.ErrNoDocuments {
		return nil
	}

	if err != nil {
		panic(err)
	}

	return &el
}

func (r *MongoDbRepository[T]) insertDefaultParam(ctx context.Context, entity *T) (bson.M, error) {
	bsonMap, err := bson.Marshal(entity)
	if err != nil {
		return nil, err
	}

	var bsonM bson.M
	err = bson.Unmarshal(bsonMap, &bsonM)
	if err != nil {
		return nil, err
	}

	helperContext(ctx, bsonM, map[string]string{"tenantId": "X-Tenant-Id", "createdBy": "X-Author", "updatedBy": "X-Author"})

	bsonM["createdAt"] = time.Now()
	bsonM["updatedAt"] = time.Now()

	return bsonM, nil
}

func (r *MongoDbRepository[T]) replaceDefaultParam(ctx context.Context, old bson.M, entity *T) (bson.M, error) {
	bsonMap, err := bson.Marshal(entity)
	if err != nil {
		return nil, err
	}

	var bsonM bson.M
	err = bson.Unmarshal(bsonMap, &bsonM)
	if err != nil {
		return nil, err
	}

	helperContext(ctx, bsonM, map[string]string{"tenantId": "X-Tenant-Id", "updatedBy": "X-Author"})
	bsonM["createdAt"] = old["createdAt"]
	bsonM["createdBy"] = old["createdBy"]
	bsonM["updatedAt"] = time.Now()

	return bsonM, nil
}

func (r *MongoDbRepository[T]) Insert(
	ctx context.Context,
	entity *T) {

	opt := options.InsertOne()
	opt.SetBypassDocumentValidation(true)

	bsonM, err := r.insertDefaultParam(ctx, entity)
	if err != nil {
		log.Fatalln(err.Error())
		panic(err)
	}

	_, err = r.collection.InsertOne(ctx, bsonM, opt)
	if err != nil {
		log.Fatalln(err.Error())
		panic(err)
	}
}

func (r *MongoDbRepository[T]) InsertAll(
	ctx context.Context,
	entities *[]T) {

	var uis []interface{}
	for _, ui := range *entities {
		bsonM, err := r.insertDefaultParam(ctx, &ui)
		if err != nil {
			log.Fatalln(err.Error())
			panic(err)
		}

		uis = append(uis, bsonM)
	}
	_, err := r.collection.InsertMany(ctx, uis)
	if err != nil {
		panic(err)
	}
}

func (r *MongoDbRepository[T]) Replace(
	ctx context.Context,
	filter map[string]interface{},
	entity *T) {

	filter["tenantId"] = ctx.(*gin.Context).Request.Header.Get("X-Tenant-Id")
	var el bson.M
	err := r.collection.FindOne(ctx, filter).Decode(&el)

	if err == mongo.ErrNoDocuments {
		return
	}

	bsonM, err := r.replaceDefaultParam(ctx, el, entity)
	if err != nil {
		panic(err)
	}

	_, err = r.collection.ReplaceOne(ctx, filter, bsonM)
	if err != nil {
		panic(err)
	}
}
