package goframework

import (
	"context"
	"time"
)

type (
	ICache interface {
		Set(ctx context.Context, key string, val interface{}, ttlIsSeconds time.Duration) error
		Get(ctx context.Context, key string, pointer interface{}) error
	}
)
