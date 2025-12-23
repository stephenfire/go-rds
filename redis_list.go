package rds

import (
	"context"
	"reflect"

	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-tools"
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

	RedisTree[K1 comparable, K2 comparable, V any] struct {
		parent       *RedisString[K1, []K2]
		child        *RedisString[K2, V]
		parentLoader MapLoader[K1, []K2]
		childLoader  MapLoader[K2, V]
		isChildNil   IsNil[V]
	}
)

func IsDefaultZero[T any](t T) bool {
	val := reflect.ValueOf(t)
	if !val.IsValid() {
		return true
	}
	return val.IsZero()
}

func NewRedisTree[K1 comparable, K2 comparable, V any](
	parentRS *RedisString[K1, []K2], parentLoader MapLoader[K1, []K2],
	childRS *RedisString[K2, V], childLoader MapLoader[K2, V],
	isChildNil ...IsNil[V]) *RedisTree[K1, K2, V] {
	rtree := &RedisTree[K1, K2, V]{
		parent:       parentRS,
		child:        childRS,
		parentLoader: parentLoader,
		childLoader:  childLoader,
	}
	if len(isChildNil) > 0 && isChildNil[0] != nil {
		rtree.isChildNil = isChildNil[0]
	}
	return rtree
}

func (t *RedisTree[K1, K2, V]) _isChildNil(v V) bool {
	if t.isChildNil == nil {
		return IsDefaultZero(v)
	}
	return t.isChildNil(v)
}

func (t *RedisTree[K1, K2, V]) Children(ctx context.Context, ks ...K1) (map[K1][]V, error) {
	childrenKeyMap, err := t.parent.GetsAndSets(ctx, t.parentLoader, ks...)
	if err != nil {
		return nil, err
	}
	if len(childrenKeyMap) == 0 {
		return nil, nil
	}
	childrenKeys := tools.AllMapValueSlices(childrenKeyMap)
	if len(childrenKeys) == 0 {
		return nil, nil
	}
	childrenMap, err := t.child.GetsAndSets(ctx, t.childLoader, childrenKeys...)
	if err != nil {
		return nil, err
	}
	if len(childrenMap) == 0 {
		return nil, nil
	}
	ret := make(map[K1][]V, len(childrenKeyMap))
	for k1, k2s := range childrenKeyMap {
		for _, k2 := range k2s {
			if child := childrenMap[k2]; t._isChildNil(child) {
				continue
			} else {
				ret[k1] = append(ret[k1], child)
			}
		}
	}
	return ret, nil
}
