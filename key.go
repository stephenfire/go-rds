package rds

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type RedisKey[K any] struct {
	client     redis.Cmdable
	keyEncoder RedisEncoder[K]
}

func (rk *RedisKey[K]) _keys(ks ...K) (keys []string, err error) {
	if len(ks) == 0 {
		return nil, nil
	}
	keys = make([]string, len(ks))
	for i, k := range ks {
		keys[i], err = rk.keyEncoder(k)
		if err != nil {
			return nil, fmt.Errorf("encode key failed: %w", err)
		}
	}
	return keys, nil
}

func (rk *RedisKey[K]) Dels(ctx context.Context, ks ...K) error {
	keys, err := rk._keys(ks...)
	if err != nil || len(keys) == 0 {
		return err
	}
	return rk.client.Del(ctx, keys...).Err()
}

func (rk *RedisKey[K]) Exist(ctx context.Context, k K) bool {
	keys, err := rk._keys(k)
	if err != nil || len(keys) == 0 {
		return false
	}
	n, err := rk.client.Exists(ctx, keys...).Result()
	return err == nil && n > 0
}

func (rk *RedisKey[K]) Key(k K) (key string, err error)         { return rk.keyEncoder(k) }
func (rk *RedisKey[K]) Keys(ks ...K) (keys []string, err error) { return rk._keys(ks...) }
