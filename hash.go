package rds

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-tools"
)

type KeyValuer[K comparable, V any] interface {
	Key() K
	Value() V
}

func ConvertKV[K comparable, V any, T KeyValuer[K, V]](values []T) []KeyValuer[K, V] {
	if values == nil {
		return nil
	}
	r := make([]KeyValuer[K, V], len(values))
	if len(values) == 0 {
		return r
	}
	for i, v := range values {
		r[i] = v
	}
	return r
}

func MapKV[K comparable, V any, T KeyValuer[K, V]](values []T) map[K]V {
	if len(values) == 0 {
		return nil
	}
	m := make(map[K]V, len(values))
	for _, value := range values {
		m[value.Key()] = value.Value()
	}
	return m
}

type RedisHasher[K comparable, V any] struct {
	client       redis.Cmdable
	fieldDecoder RedisDecoder[K]
	valueDecoder RedisDecoder[V]
	fieldEncoder RedisEncoder[K]
	valueEncoder RedisEncoder[V]
}

func NewRedisHasher[K comparable, V any](
	client redis.Cmdable,
	fieldEncoder RedisEncoder[K], fieldDecoder RedisDecoder[K],
	valueEncoder RedisEncoder[V], valueDecoder RedisDecoder[V]) *RedisHasher[K, V] {
	return &RedisHasher[K, V]{
		client:       client,
		fieldEncoder: fieldEncoder,
		fieldDecoder: fieldDecoder,
		valueEncoder: valueEncoder,
		valueDecoder: valueDecoder,
	}
}

func NewHashReader[K comparable, V any](client redis.Cmdable, fieldEncoder RedisEncoder[K],
	fieldDecoder RedisDecoder[K], valueDecoder RedisDecoder[V]) *RedisHasher[K, V] {
	return NewRedisHasher[K, V](client, fieldEncoder, fieldDecoder, nil, valueDecoder)
}

func NewStringInt64Hasher(c redis.Cmdable) *RedisHasher[string, int64] {
	return NewRedisHasher[string, int64](c,
		RedisStringEncoder, RedisStringDecoder,
		RedisInt64Encoder, RedisInt64Decoder)
}

func (t *RedisHasher[K, V]) FieldDecoder(decoder RedisDecoder[K]) *RedisHasher[K, V] {
	t.fieldDecoder = decoder
	return t
}

func (t *RedisHasher[K, V]) ValueDecoder(decoder RedisDecoder[V]) *RedisHasher[K, V] {
	t.valueDecoder = decoder
	return t
}

func (t *RedisHasher[K, V]) FieldEncoder(encoder RedisEncoder[K]) *RedisHasher[K, V] {
	t.fieldEncoder = encoder
	return t
}

func (t *RedisHasher[K, V]) ValueEncoder(encoder RedisEncoder[V]) *RedisHasher[K, V] {
	t.valueEncoder = encoder
	return t
}

func (t *RedisHasher[K, V]) _fieldStrs(fields ...K) ([]string, error) {
	var fieldStrings = make([]string, len(fields))
	var err error
	for i := 0; i < len(fields); i++ {
		fieldStrings[i], err = t.fieldEncoder(fields[i])
		if err != nil {
			return nil, fmt.Errorf("encode field failed: %w", err)
		}
	}
	return fieldStrings, nil
}

func (t *RedisHasher[K, V]) HDel(ctx context.Context, key string, fields ...K) (int64, error) {
	fieldStrings, err := t._fieldStrs(fields...)
	if err != nil {
		return 0, err
	}
	if len(fieldStrings) == 0 {
		return 0, nil
	}
	return t.HDelFieldString(ctx, key, fieldStrings...)
}

func (t *RedisHasher[K, V]) HDelFieldString(ctx context.Context, key string, fieldStrings ...string) (int64, error) {
	if len(fieldStrings) == 0 {
		return 0, nil
	}
	return t.client.HDel(ctx, key, fieldStrings...).Result()
}

func (t *RedisHasher[K, V]) HGets(ctx context.Context, key string, fields ...K) (notcached []K, cached map[K]V, err error) {
	if len(fields) == 0 {
		return nil, nil, nil
	}
	fields = tools.KS[K](fields).Dedup()
	strFields, err := t._fieldStrs(fields...)
	if err != nil {
		return nil, nil, err
	}
	var hitlist []bool
	hitlist, cached, err = t.HGetByFieldStrings(ctx, key, strFields...)
	if err != nil {
		return nil, nil, err
	}
	for i, hit := range hitlist {
		if !hit {
			notcached = append(notcached, fields[i])
		}
	}
	return notcached, cached, nil
}

// HGetsAndSets read data from hashes:key using ks as the fields. If any fields do not exist,
// use dbLoader to read the corresponding data from the database and store it in the cache.
// Note that dbLoader should not return an error if the corresponding data does not exist in the database.
func (t *RedisHasher[K, V]) HGetsAndSets(ctx context.Context, key string,
	dbLoader func(cctx context.Context, kks ...K) (map[K]V, error), ks ...K) (map[K]V, error) {
	if len(ks) == 0 {
		return nil, nil
	}
	vks := tools.KS[K](ks).Dedup().Slice()
	// get from cache
	notcached, cached, err := t.HGets(ctx, key, vks...)
	if err != nil {
		return nil, err
	}
	if len(notcached) == 0 {
		return cached, nil
	}
	if dbLoader == nil {
		panic(errors.New("dbLoader is nil"))
	}
	// load missed data from db
	tmap, err := dbLoader(ctx, notcached...)
	if err != nil {
		return nil, err
	}
	// set to cache
	_ = t.HSetsMap(ctx, key, tools.KMap[K, V](tmap).SubMap(notcached...))
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

func (t *RedisHasher[K, V]) HGetAndSet(ctx context.Context, key string,
	dbLoader func(cctx context.Context, kks ...K) (map[K]V, error), k K) (v V, err error) {
	vmap, err := t.HGetsAndSets(ctx, key, dbLoader, k)
	if err != nil {
		return v, err
	}
	v = vmap[k]
	return v, nil
}

func (t *RedisHasher[K, V]) HGet(ctx context.Context, key string, field K) (hit bool, value V, err error) {
	fs, errr := t.fieldEncoder(field)
	if errr != nil {
		err = fmt.Errorf("field encode failed: %w", errr)
		return
	}
	return t.HGetByFieldString(ctx, key, fs)
}

func (t *RedisHasher[K, V]) HGetByFieldStrings(ctx context.Context, key string, fieldStrings ...string) (hitList []bool, cached map[K]V, err error) {
	if len(fieldStrings) == 0 {
		return nil, nil, nil
	}
	hitList = make([]bool, len(fieldStrings))
	values, err := t.client.HMGet(ctx, key, fieldStrings...).Result()
	if err != nil {
		if IsRedisNil(err) {
			return hitList, nil, nil
		}
		return nil, nil, err
	}
	if len(values) == 0 {
		return hitList, nil, nil
	}
	if len(values) != len(fieldStrings) {
		return nil, nil, errors.New("invalid values number")
	}
	cached = make(map[K]V)
	for i, rval := range values {
		if rval == nil {
			continue
		}
		hitList[i] = true
		k, err := t.fieldDecoder(fieldStrings[i])
		if err != nil {
			return nil, nil, fmt.Errorf("parse key failed: %w", err)
		}
		if sv, ok := rval.(string); !ok {
			return nil, nil, ErrExpectingString
		} else {
			v, err := t.valueDecoder(sv)
			if err != nil {
				return nil, nil, err
			}
			cached[k] = v
		}
	}
	return hitList, cached, nil
}

func (t *RedisHasher[K, V]) HGetByFieldString(ctx context.Context, key string, fieldString string) (hit bool, value V, err error) {
	hits, vmap, errr := t.HGetByFieldStrings(ctx, key, fieldString)
	if errr != nil {
		err = errr
		return
	}
	k, errr := t.fieldDecoder(fieldString)
	if errr != nil {
		err = errr
		return
	}
	return hits[0], vmap[k], nil
}

func (t *RedisHasher[K, V]) HGetAll(ctx context.Context, key string) (map[K]V, error) {
	valueMap, err := t.client.HGetAll(ctx, key).Result()
	if err != nil {
		if IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(valueMap) == 0 {
		return nil, nil
	}
	ret := make(map[K]V, len(valueMap))
	for k, rv := range valueMap {
		field, err := t.fieldDecoder(k)
		if err != nil {
			return nil, err
		}
		value, err := t.valueDecoder(rv)
		if err != nil {
			return nil, err
		}
		ret[field] = value
	}
	return ret, nil
}

func (t *RedisHasher[K, V]) HKeys(ctx context.Context, key string) ([]K, error) {
	fields, err := t.client.HKeys(ctx, key).Result()
	if err != nil {
		if IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(fields) == 0 {
		return nil, nil
	}
	ret := make([]K, len(fields))
	for i, k := range fields {
		field, err := t.fieldDecoder(k)
		if err != nil {
			return nil, err
		}
		ret[i] = field
	}
	return ret, nil
}

func (t *RedisHasher[K, V]) HSet(ctx context.Context, key string, field K, value V) error {
	fieldStr, err := t.fieldEncoder(field)
	if err != nil {
		return fmt.Errorf("encode field failed: %w", err)
	}
	if fieldStr == "" {
		return errors.New("empty field name")
	}
	valueStr, err := t.valueEncoder(value)
	if err != nil {
		return fmt.Errorf("encode value failed: %w", err)
	}
	if valueStr == "" {
		return errors.New("empty value")
	}
	return t.client.HSet(ctx, key, fieldStr, valueStr).Err()
}

func (t *RedisHasher[K, V]) HSets(ctx context.Context, key string, values []KeyValuer[K, V]) error {
	if len(values) == 0 {
		return nil
	}
	var params []any
	var count int64
	for _, kv := range values {
		k := kv.Key()
		v := kv.Value()
		oneKey, err := t.fieldEncoder(k)
		if err != nil {
			return fmt.Errorf("encode field failed: %w", err)
		}
		if oneKey == "" {
			continue
		}
		oneVal, err := t.valueEncoder(v)
		if err != nil {
			return fmt.Errorf("encode value failed: %w", err)
		}
		if oneVal == "" {
			continue
		}
		params = append(params, oneKey, oneVal)
		count++
		if count%BatchSize == 0 {
			if err := t.client.HSet(ctx, key, params...).Err(); err != nil {
				return err
			}
			params = params[:0]
			count = 0
		}
	}
	if len(params) > 0 {
		return t.client.HSet(ctx, key, params...).Err()
	}
	return nil
}

func (t *RedisHasher[K, V]) HSetsMap(ctx context.Context, key string, valueMap map[K]V) error {
	if len(valueMap) == 0 {
		return nil
	}
	var params []any
	var count int64
	for k, v := range valueMap {
		oneKey, err := t.fieldEncoder(k)
		if err != nil {
			return fmt.Errorf("encode field failed: %w", err)
		}
		if oneKey == "" {
			continue
		}
		oneVal, err := t.valueEncoder(v)
		if err != nil {
			return fmt.Errorf("encode value failed: %w", err)
		}
		if oneVal == "" {
			continue
		}
		params = append(params, oneKey, oneVal)
		count++
		if count%BatchSize == 0 {
			if err := t.client.HSet(ctx, key, params...).Err(); err != nil {
				return err
			}
			params = params[:0]
			count = 0
		}
	}
	if len(params) > 0 {
		return t.client.HSet(ctx, key, params...).Err()
	}
	return nil
}

func (t *RedisHasher[K, V]) HMustSets(ctx context.Context, key string, values []KeyValuer[K, V],
	mustFields []K, placeholderVal V) error {
	if len(mustFields) == 0 {
		return nil
	}
	if len(values) == 0 {
		return t.HMustSetsMap(ctx, key, nil, mustFields, placeholderVal)
	}
	m := make(map[K]V, len(values))
	for _, value := range values {
		m[value.Key()] = value.Value()
	}
	return t.HMustSetsMap(ctx, key, m, mustFields, placeholderVal)
}

func (t *RedisHasher[K, V]) HMustSetsMap(ctx context.Context, key string, valueMap map[K]V,
	mustFields []K, placeholderVal V) error {
	if len(mustFields) == 0 {
		return nil
	}
	var params []any
	var count int64
	for _, k := range mustFields {
		oneKey, err := t.fieldEncoder(k)
		if err != nil {
			return fmt.Errorf("encode field failed: %w", err)
		}
		if oneKey == "" {
			continue
		}
		v, exist := valueMap[k]
		if !exist {
			v = placeholderVal
		}
		oneVal, err := t.valueEncoder(v)
		if err != nil {
			return fmt.Errorf("encode value failed: %w", err)
		}
		if oneVal == "" {
			continue
		}
		params = append(params, oneKey, oneVal)
		count++
		if count%BatchSize == 0 {
			if err := t.client.HSet(ctx, key, params...).Err(); err != nil {
				return err
			}
			params = params[:0]
			count = 0
		}
	}
	if len(params) > 0 {
		return t.client.HSet(ctx, key, params...).Err()
	}
	return nil
}

func (t *RedisHasher[K, V]) HExists(ctx context.Context, key string, field K) (bool, error) {
	fieldStr, err := t.fieldEncoder(field)
	if err != nil {
		return false, fmt.Errorf("encode field failed: %w", err)
	}
	if fieldStr == "" {
		return false, errors.New("empty field name")
	}
	return t.client.HExists(ctx, key, fieldStr).Result()
}

func (t *RedisHasher[K, V]) HExpire(ctx context.Context, key string, seconds int64, fields ...K) ([]bool, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	fieldStrs, err := t._fieldStrs(fields...)
	if err != nil {
		return nil, err
	}
	results, err := t.client.HExpire(ctx, key, time.Duration(seconds)*time.Second, fieldStrs...).Result()
	if err != nil {
		return nil, err
	}
	return tools.SsToTs(func(s int64) bool { return s == 1 }, results...), nil
}

func (t *RedisHasher[K, V]) loadAndSetAll(ctx context.Context, key string,
	allLoader func(ctx context.Context) (map[K]V, error), flagField K) (all map[K]V, err error) {
	if allLoader == nil {
		panic(errors.New("allLoader is nil"))
	}
	all, err = allLoader(ctx)
	if err != nil {
		return nil, err
	}
	var dv V
	km := tools.KMap[K, V](all).Put(flagField, dv)
	_ = t.HSetsMap(ctx, key, km)
	return all, nil
}

// HGetsAndSetsIfNA 检查key是否存在，如果不存在则通过allLoader获取所有key的数据并保存
// redis的hash无法存储0个field，所以当使用hash存储对象时，如果不做特殊处理，将无法缓存不存在的key（如：数据库中不存在）。
// 此时通过使用在field中增加一个特殊值flag（不会与真实filed重复），用来保证没有其他field时也可以使得key在redis中。
// 而flag对应的值则是V的缺省值。
// 读取时，可以通过HMGet每次都将flag读取出来，一旦flag不存在，就说明key不存在。如果flag存在，而可以说明被读取的field是否真实存在。
func (t *RedisHasher[K, V]) HGetsAndSetsIfNA(ctx context.Context, key string,
	allLoader func(ctx context.Context) (map[K]V, error), flagField K, kks ...K) (map[K]V, error) {
	if len(kks) == 0 {
		return nil, nil
	}
	vks := tools.KS[K](kks).Append(flagField).Dedup().Slice()
	_, cached, err := t.HGets(ctx, key, vks...)
	if err != nil {
		return nil, err
	}
	if _, exist := cached[flagField]; exist {
		// key exists
		delete(cached, flagField)
		return cached, nil
	}
	all, err := t.loadAndSetAll(ctx, key, allLoader, flagField)
	if err != nil {
		return nil, err
	}
	return (tools.KMap[K, V])(all).SubMap(kks...), nil
}

func (t *RedisHasher[K, V]) HGetAndSetIfNA(ctx context.Context, key string,
	allLoader func(ctx context.Context) (map[K]V, error), flagField K, k K) (v V, err error) {
	m, err := t.HGetsAndSetsIfNA(ctx, key, allLoader, flagField, k)
	if err != nil {
		return v, err
	}
	if m == nil {
		return v, nil
	}
	return m[k], nil
}

func (t *RedisHasher[K, V]) HGetAllAndSetIfNA(ctx context.Context, key string,
	allLoader func(ctx context.Context) (map[K]V, error), flagField K) (map[K]V, error) {
	cached, err := t.HGetAll(ctx, key)
	if err != nil {
		return nil, err
	}
	if _, exist := cached[flagField]; exist {
		// key exists
		delete(cached, flagField)
		return cached, nil
	}
	all, err := t.loadAndSetAll(ctx, key, allLoader, flagField)
	if err != nil {
		return nil, err
	}
	delete(all, flagField)
	return all, nil
}

func (t *RedisHasher[K, V]) HKeysAndSetIfNA(ctx context.Context, key string,
	allLoader func(ctx context.Context) (map[K]V, error), flagField K) (fields []K, err error) {
	defer func() {
		if err == nil {
			fields = tools.KS[K](fields).Remove(flagField)
			return
		}
	}()
	fields, err = t.HKeys(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(fields) > 0 {
		return fields, nil
	}
	all, err := t.loadAndSetAll(ctx, key, allLoader, flagField)
	if err != nil {
		return nil, err
	}
	delete(all, flagField)
	return tools.KMap[K, V](all).Keys(), nil
}
