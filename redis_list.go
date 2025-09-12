package rds

import (
	"context"

	"github.com/redis/go-redis/v9"
)

type RedisBatchValue[V any] struct {
	batchSize int // default consts.BatchSize
	encoder   RedisEncoder[V]
	decoder   RedisDecoder[V]
}

func (b *RedisBatchValue[V]) _batchSize() int {
	if b.batchSize <= 0 {
		return BatchSize
	}
	return b.batchSize
}

func (b *RedisBatchValue[V]) _batchValues(op func(vals ...interface{}) error, vs ...V) error {
	batchSize := b._batchSize()
	batchIt := NewBatchIterator(vs, batchSize)
	values := make([]interface{}, 0, min(len(vs), batchSize))
	var strVal string
	var err error
	for batchIt.HasNext() {
		values = values[:0]
		bvs := batchIt.Next()
		for _, bv := range bvs {
			strVal, err = b.encoder(bv)
			if err != nil {
				return err
			}
			if strVal != "" {
				values = append(values, strVal)
			}
		}
		if len(values) == 0 {
			continue
		}
		err = op(values...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *RedisBatchValue[V]) _batchRun(op func(vals ...interface{}) (int64, error), vs ...V) (int64, error) {
	var sum, n int64
	var err error
	errr := b._batchValues(func(vals ...interface{}) error {
		n, err = op(vals...)
		sum += n
		return err
	}, vs...)
	return sum, errr
}

type RedisList[V any] struct {
	client redis.Cmdable
	RedisBatchValue[V]
}

func (l *RedisList[V]) WithBatchSize(size int) *RedisList[V] { l.batchSize = size; return l }

func (l *RedisList[V]) RPush(ctx context.Context, key string, vs ...V) (int64, error) {
	if len(vs) == 0 {
		return 0, nil
	}
	return l._batchRun(func(vals ...interface{}) (int64, error) {
		return l.client.RPush(ctx, key, vals...).Result()
	}, vs...)
}

func (l *RedisList[V]) LRange(ctx context.Context, key string, start, stop int64) ([]V, error) {
	strs, err := l.client.LRange(ctx, key, start, stop).Result()
	if err != nil {
		if IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	vs := make([]V, 0, len(strs))
	for _, str := range strs {
		v, err := l.decoder(str)
		if err != nil {
			return nil, err
		}
		vs = append(vs, v)
	}
	return vs, nil
}

type (
	MapLoader[K comparable, V any] func(ctx context.Context, ks ...K) (map[K]V, error)

	IsNil[T any] func(T) bool
)
