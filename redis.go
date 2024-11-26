package goframework

func newRedisClient(opts *redis.Options) *redis.Client {
	return redis.NewClient(opts)
}
