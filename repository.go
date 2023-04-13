package goframework

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDbRepository[T interface{}] struct {
	collection *mongo.Collection
}

func NewMongoDbRepository[T interface{}](
	db *mongo.Database,
) IRepository[T] {
	var r T
	coll := db.Collection(strings.ToLower(reflect.TypeOf(r).Name()))
	return &MongoDbRepository[T]{
		collection: coll,
	}
}

func (r *MongoDbRepository[T]) GetAll(
	ctx context.Context,
	filter map[string]interface{}) *[]T {

	if tenantId := getContextHeader(ctx, "X-Tenant-Id"); tenantId != "" {
		filter["$or"] = bson.A{
			bson.D{{"tenantId", uuid.MustParse(tenantId)}},
			bson.D{{"tenantId", uuid.MustParse("00000000-0000-0000-0000-000000000000")}},
		}
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(obj, err)
	}

	cur, err := r.collection.Find(getContext(ctx), filter)
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

	if tenantId := getContextHeader(ctx, "X-Tenant-Id"); tenantId != "" {
		filter["$or"] = bson.A{
			bson.D{{"tenantId", uuid.MustParse(tenantId)}},
			bson.D{{"tenantId", uuid.MustParse("00000000-0000-0000-0000-000000000000")}},
		}
	}

	op := options.Find()
	op.SetSkip(skip)
	op.SetLimit(take)

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(obj, err)
	}

	cur, err := r.collection.Find(getContext(ctx), filter, op)

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

	if tenantId := getContextHeader(ctx, "X-Tenant-Id"); tenantId != "" {
		filter["$or"] = bson.A{
			bson.D{{"tenantId", uuid.MustParse(tenantId)}},
			bson.D{{"tenantId", uuid.MustParse("00000000-0000-0000-0000-000000000000")}},
		}
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(obj, err)
	}

	err := r.collection.FindOne(getContext(ctx), filter).Decode(&el)

	if err == mongo.ErrNoDocuments {
		return nil
	}

	if err != nil {
		panic(err)
	}

	return &el
}

func (r *MongoDbRepository[T]) insertDefaultParam(ctx context.Context, entity *T) (bson.M, error) {
	bsonMap, err := bson.MarshalWithRegistry(mongoRegistry, entity)
	if err != nil {
		return nil, err
	}

	var bsonM bson.M
	err = bson.Unmarshal(bsonMap, &bsonM)
	if err != nil {
		return nil, err
	}
	helperContext(ctx, bsonM, map[string]string{"createdBy": "X-Author", "updatedBy": "X-Author"})
	if tenantid := getContextHeader(ctx, "X-Tenant-Id"); tenantid != "" {
		bsonM["tenantId"] = uuid.MustParse(tenantid)
	}
	bsonM["createdAt"] = time.Now()
	bsonM["updatedAt"] = time.Now()

	return bsonM, nil
}

func (r *MongoDbRepository[T]) replaceDefaultParam(ctx context.Context, old bson.M, entity *T) (bson.M, error) {
	bsonMap, err := bson.MarshalWithRegistry(mongoRegistry, entity)
	if err != nil {
		return nil, err
	}

	var bsonM bson.M
	err = bson.Unmarshal(bsonMap, &bsonM)
	if err != nil {
		return nil, err
	}

	bsonM["tenantId"] = old["tenantId"]
	bsonM["createdAt"] = old["createdAt"]
	bsonM["createdBy"] = old["createdBy"]
	bsonM["updatedAt"] = time.Now()
	bsonM["updatedBy"] = getContextHeader(ctx, "X-Author")
	return bsonM, nil
}

func (r *MongoDbRepository[T]) Insert(
	ctx context.Context,
	entity *T) error {

	opt := options.InsertOne()
	opt.SetBypassDocumentValidation(true)

	bsonM, err := r.insertDefaultParam(ctx, entity)
	if err != nil {
		return err
	}

	_, err = r.collection.InsertOne(ctx, bsonM, opt)
	if err != nil {
		return err
	}

	return nil
}

func (r *MongoDbRepository[T]) InsertAll(
	ctx context.Context,
	entities *[]T) error {

	var uis []interface{}
	for _, ui := range *entities {
		bsonM, err := r.insertDefaultParam(ctx, &ui)
		if err != nil {
			return err
		}

		uis = append(uis, bsonM)
	}
	_, err := r.collection.InsertMany(getContext(ctx), uis)
	if err != nil {
		return err
	}

	return nil
}

func (r *MongoDbRepository[T]) Replace(
	ctx context.Context,
	filter map[string]interface{},
	entity *T) error {

	if tenantId := getContextHeader(ctx, "X-Tenant-Id"); tenantId != "" {
		filter["tenantId"] = uuid.MustParse(tenantId)
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(obj, err)
	}

	var el bson.M
	err := r.collection.FindOne(getContext(ctx), filter).Decode(&el)

	if err == mongo.ErrNoDocuments {
		return err
	}

	bsonM, err := r.replaceDefaultParam(ctx, el, entity)
	if err != nil {
		return err
	}

	_, err = r.collection.ReplaceOne(getContext(ctx), filter, bsonM)
	if err != nil {
		return err
	}

	return nil
}

func (r *MongoDbRepository[T]) Update(
	ctx context.Context,
	filter map[string]interface{},
	fields interface{}) error {

	if tenantId := getContextHeader(ctx, "X-Tenant-Id"); tenantId != "" {
		filter["tenantId"] = uuid.MustParse(tenantId)
	}

	setBson := structToBson(fields)

	if author := getContextHeader(ctx, "X-Author"); author != "" {
		setBson["updatedBy"] = author
		setBson["updatedAt"] = time.Now()
	}

	if os.Getenv("env") == "local" {
		_, obj, err := bson.MarshalValue(filter)
		fmt.Print(obj, err)
	}

	re, err := r.collection.UpdateOne(getContext(ctx), filter, map[string]interface{}{"$set": setBson})

	if err != nil {
		return err
	}

	if re.MatchedCount == 0 {
		return fmt.Errorf("MatchedCountZero")
	}

	return nil
}
