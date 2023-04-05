package goframework

type FResult[T interface{}] struct {
	Code int
	Data T
}

func NewFResult[T interface{}](code int, data T) *FResult[T] {
	return &FResult[T]{Code: code, Data: data}
}
