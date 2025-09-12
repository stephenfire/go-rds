package rds

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type RedisSet[V any] struct {
	client redis.Cmdable
	RedisBatchValue[V]
}

func NewRedisSet[V any](client redis.Cmdable, encoder RedisEncoder[V], decoder RedisDecoder[V]) *RedisSet[V] {
	return &RedisSet[V]{
		client: client,
		RedisBatchValue: RedisBatchValue[V]{
			batchSize: 0,
			encoder:   encoder,
			decoder:   decoder,
		},
	}
}

func (s *RedisSet[V]) WithBatchSize(size int) *RedisSet[V] { s.batchSize = size; return s }

func (s *RedisSet[V]) SRem(ctx context.Context, key string, vs ...V) (int64, error) {
	if len(vs) == 0 {
		return 0, nil
	}
	return s._batchRun(func(vals ...interface{}) (int64, error) {
		return s.client.SRem(ctx, key, vals...).Result()
	}, vs...)
}

func (s *RedisSet[V]) SIsMember(ctx context.Context, key string, v V) (bool, error) {
	value, err := s.encoder(v)
	if err != nil {
		return false, err
	}
	return s.client.SIsMember(ctx, key, value).Result()
}

func (s *RedisSet[V]) SAdd(ctx context.Context, key string, vs ...V) (int64, error) {
	if len(vs) == 0 {
		return 0, nil
	}
	return s._batchRun(func(vals ...interface{}) (int64, error) {
		return s.client.SAdd(ctx, key, vals...).Result()
	}, vs...)
}

func (s *RedisSet[V]) SAddWithEmpty(ctx context.Context, key string, emptyValue V, vs ...V) (int64, error) {
	if len(vs) == 0 {
		vs = append(vs, emptyValue)
	}
	return s._batchRun(func(vals ...interface{}) (int64, error) {
		return s.client.SAdd(ctx, key, vals...).Result()
	}, vs...)
}
