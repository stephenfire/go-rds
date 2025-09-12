package rds

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-tools"
)

type RedisString[K comparable, V any] struct {
	RedisKey[K]
	valueEncoder RedisEncoder[V]
	valueDecoder RedisDecoder[V]
}

func NewRedisString[K comparable, V any](client redis.Cmdable, keyEncoder RedisEncoder[K],
	valueEncoder RedisEncoder[V], valueDecoder RedisDecoder[V]) *RedisString[K, V] {
	return &RedisString[K, V]{
		RedisKey: RedisKey[K]{
			client:     client,
			keyEncoder: keyEncoder,
		},
		valueEncoder: valueEncoder,
		valueDecoder: valueDecoder,
	}
}

func NewStringReader[K comparable, V any](client redis.Cmdable, keyEncoder RedisEncoder[K],
	valueDecoder RedisDecoder[V]) *RedisString[K, V] {
	return &RedisString[K, V]{
		RedisKey: RedisKey[K]{
			client:     client,
			keyEncoder: keyEncoder,
		},
		valueDecoder: valueDecoder,
	}
}

func NewStringWriter[K comparable, V any](client redis.Cmdable, keyEncoder RedisEncoder[K],
	valueEncoder RedisEncoder[V]) *RedisString[K, V] {
	return &RedisString[K, V]{
		RedisKey: RedisKey[K]{
			client:     client,
			keyEncoder: keyEncoder,
		},
		valueEncoder: valueEncoder,
	}
}

func (s *RedisString[K, V]) Client(client redis.Cmdable) *RedisString[K, V] {
	s.client = client
	return s
}

func (s *RedisString[K, V]) KeyEncoder(encoder RedisEncoder[K]) *RedisString[K, V] {
	s.keyEncoder = encoder
	return s
}

func (s *RedisString[K, V]) ValueEncoder(encoder RedisEncoder[V]) *RedisString[K, V] {
	s.valueEncoder = encoder
	return s
}

func (s *RedisString[K, V]) ValueDecoder(decoder RedisDecoder[V]) *RedisString[K, V] {
	s.valueDecoder = decoder
	return s
}

func (s *RedisString[K, V]) Gets(ctx context.Context, ks ...K) (notcached []K, cached map[K]V, err error) {
	if len(ks) == 0 {
		return nil, nil, nil
	}
	kks := tools.KS[K](ks).Dedup()
	keys := make([]string, len(kks))
	for i, k := range kks {
		keys[i], err = s.keyEncoder(k)
		if err != nil {
			return nil, nil, fmt.Errorf("encode key failed: %w", err)
		}
	}
	redisVals, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		if IsRedisNil(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	if len(redisVals) != len(kks) {
		return nil, nil, errors.New("values length mismatch")
	}
	for i, redisVal := range redisVals {
		exist, t, err := RedisValueDecode(redisVal, s.valueDecoder)
		if err != nil {
			return nil, nil, fmt.Errorf("decode value of key:%s failed: %w", keys[i], err)
		}
		if !exist {
			notcached = append(notcached, kks[i])
		} else {
			if cached == nil {
				cached = make(map[K]V, len(kks))
			}
			cached[kks[i]] = t
		}
	}
	return notcached, cached, nil
}

func (s *RedisString[K, V]) Get(ctx context.Context, k K) (exist bool, t V, err error) {
	key, err := s.keyEncoder(k)
	if err != nil {
		return false, t, fmt.Errorf("encode key failed: %w", err)
	}
	redisVal, err := s.client.Get(ctx, key).Result()
	if err != nil {
		if IsRedisNil(err) {
			return false, t, nil
		}
		return false, t, err
	}
	t, err = s.valueDecoder(redisVal)
	return true, t, err
}

func (s *RedisString[K, V]) Sets(ctx context.Context, values map[K]V) error {
	if len(values) == 0 {
		return nil
	}
	var redisVals []any
	for k, v := range values {
		key, err := s.keyEncoder(k)
		if err != nil {
			return fmt.Errorf("encode key failed: %w", err)
		}
		strVal, err := s.valueEncoder(v)
		if err != nil {
			return fmt.Errorf("encode value failed: %w", err)
		}
		redisVals = append(redisVals, key, strVal)
	}
	if len(redisVals) == 0 {
		return nil
	}
	return s.client.MSet(ctx, redisVals...).Err()
}

func (s *RedisString[K, V]) Set(ctx context.Context, k K, v V, expire ...time.Duration) error {
	key, err := s.keyEncoder(k)
	if err != nil {
		return fmt.Errorf("encode key failed: %w", err)
	}
	strVal, err := s.valueEncoder(v)
	if err != nil {
		return fmt.Errorf("encode value failed: %w", err)
	}
	ex := time.Duration(0)
	if len(expire) > 0 {
		ex = expire[0]
	}
	return s.client.Set(ctx, key, strVal, ex).Err()
}

func (s *RedisString[K, V]) GetsAndSets(ctx context.Context,
	dbLoader MapLoader[K, V], ks ...K) (map[K]V, error) {
	if len(ks) == 0 {
		return nil, nil
	}
	vks := tools.KS[K](ks).Dedup().Slice()
	// get from cache
	notcached, cached, err := s.Gets(ctx, vks...)
	if err != nil {
		return nil, err
	}
	if len(notcached) == 0 {
		return cached, nil
	}
	if dbLoader == nil {
		panic(errors.New("database value loader is missing"))
	}
	// load missed from db
	tmap, err := dbLoader(ctx, notcached...)
	if err != nil {
		return nil, err
	}
	// set to cache
	_ = s.Sets(ctx, tools.KMap[K, V](tmap).SubMap(notcached...))
	// prepare return value
	if len(tmap) > 0 {
		if cached == nil {
			cached = make(map[K]V, len(tmap))
		}
		for k, v := range tmap {
			cached[k] = v
		}
	}
	return cached, nil
}

func (s *RedisString[K, V]) GetAndSet(ctx context.Context,
	dbLoader func(cctx context.Context, kks ...K) (map[K]V, error), k K) (v V, err error) {
	var rmap map[K]V
	rmap, err = s.GetsAndSets(ctx, dbLoader, k)
	if err != nil {
		return
	}
	v = rmap[k]
	return v, nil
}
