package goframework

type FResult struct {
	Code int
	Data any
}

func NewFResult(code int, data any) *FResult {
	return &FResult{Code: code, Data: data}
}
