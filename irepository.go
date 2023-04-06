package goframework

import "context"

type IRepository[T interface{}] interface {
	GetAll(ctx context.Context,
		filter map[string]interface{}) *[]T
	GetAllSkipTake(ctx context.Context,
		filter map[string]interface{},
		skip int64,
		take int64) *[]T
	GetFirst(ctx context.Context,
		filter map[string]interface{}) *T
	Insert(ctx context.Context,
		entity *T) error
	InsertAll(ctx context.Context,
		entities *[]T) error
	Replace(ctx context.Context,
		filter map[string]interface{},
		entity *T) error
}
