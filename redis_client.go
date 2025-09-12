package rds

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-common"
	"github.com/stephenfire/go-common/log"
	"github.com/stephenfire/go-tools"
)

const (
	RedisTimeout      = 10 * time.Second
	RedisReadTimeout  = 2 * time.Second
	RedisWriteTimeout = 5 * time.Second
)

var (
	ErrInvalidValue    = errors.New("rds: invalid value object")
	ErrExpectingString = errors.New("rds: expecting string")
)

type RedisClient struct {
	*redis.Client
	version common.Version
}

func ConnectRedis(ctx context.Context, url string) (*RedisClient, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}
	opts.ReadTimeout = RedisReadTimeout
	opts.WriteTimeout = RedisWriteTimeout
	client := &RedisClient{Client: redis.NewClient(opts), version: 0}
	if err = client._info(ctx); err != nil {
		log.Warnf("%s connected, get version error: %v", client, err)
	} else {
		log.Debugf("%s connected", client)
	}
	return client, nil
}

func (c *RedisClient) Version() common.Version { return c.version }

func (c *RedisClient) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s@v%s", c.Client.String(), c.version)
}

func (c *RedisClient) _info(ctx context.Context) error {
	info, err := c.Client.Info(ctx, "server").Result()
	if err != nil {
		return err
	}
	ss, err := tools.S(info).SplitLines()
	if err != nil {
		return err
	}
	for _, s := range ss {
		parts := strings.SplitN(strings.TrimSpace(s), ":", 2)
		if len(parts) != 2 {
			continue
		}
		if tools.S(parts[0]).Trim().ToLower() == "redis_version" {
			v, err := common.NewVersion(tools.S(parts[1]).Trim().String())
			if err != nil {
				return err
			}
			c.version = v
		}
	}
	return nil
}

func (c *RedisClient) _encode(value any) (string, error) { return RedisEncode(value) }

func (c *RedisClient) _decode(typ reflect.Type, strVal string) (any, error) {
	return RedisDecode(typ, strVal)
}

func (c *RedisClient) C() *redis.Client { return c.Client }

func (c *RedisClient) Exist(ctx context.Context, key string) bool {
	n, err := c.C().Exists(ctx, key).Result()
	return err == nil && n > 0
}

func (c *RedisClient) Set(ctx context.Context, key string, value any, expire ...time.Duration) error {
	strVal, err := c._encode(value)
	if err != nil {
		return err
	}
	ex := time.Duration(0)
	if len(expire) > 0 {
		ex = expire[0]
	}
	return c.C().Set(ctx, key, strVal, ex).Err()
}

func (c *RedisClient) SetValues(ctx context.Context, pairs ...any) error {
	if len(pairs) == 0 {
		return nil
	}
	n := len(pairs)
	if n%2 != 0 {
		panic(errors.New("invalid key-value pair number"))
	}
	var strVal string
	var err error
	values := make([]any, 0, len(pairs))
	for i := 0; i < n; i += 2 {
		k, ok := pairs[i].(string)
		if !ok {
			return errors.New("key must be string")
		}
		k = strings.TrimSpace(k)
		if k == "" {
			return errors.New("invalid key")
		}
		strVal, err = c._encode(pairs[i+1])
		if err != nil {
			return err
		}
		values = append(values, k, strVal)
	}
	return c.MSet(ctx, values...).Err()
}

func (c *RedisClient) Get(ctx context.Context, key string, typ reflect.Type) (any, bool, error) {
	strVal, err := c.C().Get(ctx, key).Result()
	if err != nil {
		if IsRedisNil(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	v, err := c._decode(typ, strVal)
	return v, true, err
}

// GetValues returns
// hitList: same length as the input keys, indicates whether each key hit the cache
// values: array of decoded values or nil (not hit or cached nil)
func (c *RedisClient) GetValues(ctx context.Context, typ reflect.Type,
	keys ...string) (hitList []bool, values any, errr error) {
	if typ.Kind() != reflect.Pointer || typ.Elem().Kind() == reflect.Pointer {
		panic(fmt.Errorf("a type of pointer to a type needed, got: %s", typ.String()))
	}
	if len(keys) == 0 {
		return nil, nil, nil
	}
	hitList = make([]bool, len(keys))
	redisVals, err := c.MGet(ctx, keys...).Result()
	if err != nil {
		if IsRedisNil(err) {
			return hitList, nil, nil
		}
		return nil, nil, err
	}
	retVal := reflect.MakeSlice(reflect.SliceOf(typ), len(redisVals), len(redisVals))
	for i, rval := range redisVals {
		if rval == nil {
			continue
		}
		hitList[i] = true
		if rv, ok := rval.(string); !ok {
			return nil, nil, ErrExpectingString
		} else {
			v, err := c._decode(typ, rv)
			if err != nil {
				return nil, nil, err
			}
			vv := reflect.ValueOf(v)
			if !vv.IsNil() {
				elemVal := retVal.Index(i)
				elemVal.Set(vv)
			}
		}
	}
	return hitList, retVal.Interface(), nil
}

func (c *RedisClient) GetIds(ctx context.Context, key string) ([]int64, bool, error) {
	idsVal, exist, err := c.Get(ctx, key, tools.Int64SliceType)
	if err != nil || !exist || idsVal == nil {
		return nil, exist, err
	}
	ids := idsVal.([]int64)
	return ids, true, nil
}

func (c *RedisClient) GetStringWithNil(ctx context.Context, key string) (string, error) {
	strVal, err := c.C().Get(ctx, key).Result()
	if err != nil {
		if IsRedisNil(err) {
			return "", nil
		}
		return "", err
	}
	return strVal, nil
}

func (c *RedisClient) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	_, err := c.C().Del(ctx, keys...).Result()
	return err
}

func (c *RedisClient) HSet(ctx context.Context, key, field string, value any) error {
	strVal, err := c._encode(value)
	if err != nil {
		return err
	}
	_, err = c.C().HSet(ctx, key, field, strVal).Result()
	return err
}

func (c *RedisClient) HGet(ctx context.Context, key, field string, typ reflect.Type) (any, bool, error) {
	strVal, err := c.C().HGet(ctx, key, field).Result()
	if err != nil {
		if IsRedisNil(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	v, err := c._decode(typ, strVal)
	return v, true, err
}

func (c *RedisClient) HDel(ctx context.Context, key string, fields ...string) error {
	_, err := c.C().HDel(ctx, key, fields...).Result()
	return err
}

func (c *RedisClient) HSetsOnce(ctx context.Context, key string, fieldValues ...any) error {
	if len(fieldValues) == 0 {
		return nil
	}
	if len(fieldValues)%2 != 0 {
		return errors.New("invalid number of HSetsOnce")
	}
	var params []string
	for i := 0; i < len(fieldValues); i += 2 {
		fieldVal := reflect.ValueOf(fieldValues[i])
		if fieldVal.Kind() != reflect.String || fieldVal.String() == "" {
			return errors.New("field name of hashes must be a none-null string")
		}
		strVal, err := c._encode(fieldValues[i+1])
		if err != nil {
			return fmt.Errorf("encode value failed: %w", err)
		}
		params = append(params, fieldVal.String(), strVal)
	}
	return c.C().HSet(ctx, key, params).Err()
}

func (c *RedisClient) HSets(ctx context.Context, key string, fieldValues ...any) error {
	if len(fieldValues) == 0 {
		return c.Del(ctx, key)
	}
	if len(fieldValues)%2 != 0 {
		return errors.New("invalid number of HSets")
	}
	size := ModelsBatchSize << 1
	var params []any
	for len(fieldValues) > 0 {
		if size <= len(fieldValues) {
			params = fieldValues[:size]
			fieldValues = fieldValues[size:]
		} else {
			params = fieldValues
			fieldValues = fieldValues[:0]
		}
		if err := c.HSetsOnce(ctx, key, params...); err != nil {
			return err
		}
	}
	return nil
}

func (c *RedisClient) HSetMap(ctx context.Context, key string, m map[string]any) error {
	if len(m) == 0 {
		return c.Del(ctx, key)
	}
	var params []any
	var count int64
	for k, v := range m {
		strVal, err := c._encode(v)
		if err != nil {
			return fmt.Errorf("encode value failed: %w", err)
		}
		params = append(params, k, strVal)
		count++
		if count%ModelsBatchSize == 0 {
			if err := c.C().HSet(ctx, key, params...).Err(); err != nil {
				return err
			}
			params = params[:0]
			count = 0
		}
	}
	if len(params) > 0 {
		return c.C().HSet(ctx, key, params...).Err()
	}
	return nil
}

func parseRedisValueBy[T any](val any, parser func(strVal string) (T, error)) (t T, err error) {
	if val == nil {
		return
	}
	if sv, ok := val.(string); !ok {
		err = ErrExpectingString
		return
	} else {
		return parser(sv)
	}
}

func (c *RedisClient) _parseValue(typ reflect.Type, val any) (any, error) {
	return parseRedisValueBy[any](val, func(strVal string) (any, error) {
		return c._decode(typ, strVal)
	})
}

// HGetsAll returns map[string]anyOftyp
func (c *RedisClient) HGetsAll(ctx context.Context, key string, typ reflect.Type) (any, error) {
	valueMap, err := c.C().HGetAll(ctx, key).Result()
	if err != nil {
		if IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(valueMap) == 0 {
		return nil, nil
	}
	retMap := reflect.MakeMapWithSize(reflect.MapOf(tools.StringType, typ), len(valueMap))
	for k, rv := range valueMap {
		v, err := c._decode(typ, rv)
		if err != nil {
			return nil, err
		}
		vv := reflect.ValueOf(v)
		retMap.SetMapIndex(reflect.ValueOf(k), vv)
	}
	return retMap.Interface(), nil
}

// HGets returns cached as map[string]anyOftyp
func (c *RedisClient) HGets(ctx context.Context, key string, typ reflect.Type,
	fields ...string) (hitList []bool, cached any, err error) {
	if len(fields) == 0 {
		return nil, nil, nil
	}
	hitList = make([]bool, len(fields))
	values, err := c.C().HMGet(ctx, key, fields...).Result()
	if err != nil {
		if IsRedisNil(err) {
			return hitList, nil, nil
		}
		return nil, nil, err
	}
	if len(values) == 0 {
		return hitList, nil, nil
	}
	retVal := reflect.MakeMapWithSize(reflect.MapOf(tools.StringType, typ), len(fields))
	for i, rval := range values {
		if rval == nil {
			continue
		}
		hitList[i] = true
		v, err := c._parseValue(typ, rval)
		if err != nil {
			return nil, nil, err
		}
		vv := reflect.ValueOf(v)
		retVal.SetMapIndex(reflect.ValueOf(fields[i]), vv)
	}
	return hitList, retVal.Interface(), nil
}

func (c *RedisClient) HDels(ctx context.Context, key string, fields ...string) error {
	return c.C().HDel(ctx, key, fields...).Err()
}

func (c *RedisClient) SetZSet(ctx context.Context, key string, zs ...redis.Z) error {
	if len(zs) == 0 {
		if err := c.ZAdd(ctx, key, EmptyZ).Err(); err != nil {
			return err
		}
	} else {
		batchIt := NewBatchIterator(zs, BatchSize)
		for batchIt.HasNext() {
			bzs := batchIt.Next()
			if err := c.ZAdd(ctx, key, bzs...).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *RedisClient) ResetZSet(ctx context.Context, key string, zs ...redis.Z) error {
	pipe := c.C().Pipeline()
	pipe.Del(ctx, key)
	if len(zs) == 0 {
		pipe.ZAdd(ctx, key, EmptyZ)
	} else {
		batchIt := NewBatchIterator(zs, BatchSize)
		for batchIt.HasNext() {
			bzs := batchIt.Next()
			pipe.ZAdd(ctx, key, bzs...)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *RedisClient) ZScoreByPaddedIds(ctx context.Context, key string, paddedIds ...string) (map[int64]int64, error) {
	if len(paddedIds) == 0 {
		return nil, nil
	}
	scores, err := c.ZMScore(ctx, key, paddedIds...).Result()
	if err != nil {
		if IsRedisNil(err) {
			return map[int64]int64{}, nil
		}
		return nil, err
	}
	m := make(map[int64]int64)
	for i, paddedId := range paddedIds {
		score := tools.F(scores[i]).MustInt()
		if score < 0 {
			continue
		}
		m[tools.S(paddedId).MustID().Int()] = score
	}
	return m, nil
}

// InitStreamGroup workers should call this at start procedure
func (c *RedisClient) InitStreamGroup(ctx context.Context, streamName string, groupName string) error {
	typ, err := c.Type(ctx, streamName).Result()
	if err != nil {
		return err
	}
	if typ != TypeKeyNone && typ != TypeKeyStream {
		return fmt.Errorf("%s should be a stream, but %s", streamName, typ)
	}
	status, err := c.XGroupCreateMkStream(context.Background(), streamName, groupName, "0").Result()
	if (err == nil && status == RdbStatusOK) ||
		(err != nil && strings.Contains(err.Error(), ErrBUSYGROUPKeyword)) {
		return nil
	} else {
		return fmt.Errorf("create group failed, status:%s err:%w", status, err)
	}
}

func (c *RedisClient) QueuePushMaxLen(ctx context.Context, stream string, maxLen int64, values ...map[string]any) error {
	if len(values) == 0 {
		return nil
	}
	xadd := func(cmd redis.Cmdable, v map[string]any) *redis.StringCmd {
		if len(v) == 0 {
			return nil
		}
		return cmd.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			MaxLen: maxLen,
			Approx: true,
			Values: v,
		})
	}
	if len(values) == 1 {
		return xadd(c.C(), values[0]).Err()
	}
	pipe := c.C().Pipeline()
	for _, vv := range values {
		xadd(pipe, vv)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *RedisClient) QueuePush(ctx context.Context, stream string, minId string, values ...map[string]interface{}) error {
	if len(values) == 0 {
		return nil
	}
	approx := minId != ""
	if len(values) == 1 {
		if len(values[0]) == 0 {
			return nil
		}
		_, err := c.XAdd(ctx, &redis.XAddArgs{
			Stream:     stream,
			NoMkStream: false,
			MaxLen:     0,
			MinID:      minId,
			Approx:     approx,
			Limit:      0,
			ID:         "",
			Values:     values[0],
		}).Result()
		return err
	}
	pipe := c.C().Pipeline()
	for _, obj := range values {
		if len(obj) == 0 {
			continue
		}
		_ = pipe.XAdd(ctx, &redis.XAddArgs{
			Stream:     stream,
			NoMkStream: false,
			MaxLen:     0,
			MinID:      minId,
			Approx:     approx,
			Limit:      0,
			ID:         "",
			Values:     obj,
		})
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *RedisClient) _returnXMessages(xstreams []redis.XStream, err error) ([]redis.XMessage, error) {
	if err != nil {
		return nil, err
	}
	var msg []redis.XMessage
	if len(xstreams) > 0 {
		msg = xstreams[0].Messages
	}
	return msg, nil
}

func (c *RedisClient) QueueRead(ctx context.Context, stream string, timeout time.Duration,
	count int64, startId ...string) ([]redis.XMessage, error) {
	id := "$"
	if len(startId) > 0 && len(startId[0]) > 0 {
		s0 := tools.S(startId[0]).Trim()
		if s0.IsValid() {
			id = s0.String()
		}
	}
	xstreams, err := c.XRead(ctx, &redis.XReadArgs{
		Streams: []string{stream, id},
		Count:   count,
		Block:   timeout,
	}).Result()
	return c._returnXMessages(xstreams, err)
}

func (c *RedisClient) QueueReadGroup(ctx context.Context, stream, group, worker string,
	timeout time.Duration, count int64) ([]redis.XMessage, error) {
	xstreams, err := c.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: worker,
		Streams:  []string{stream, ">"},
		Count:    count,
		Block:    timeout,
		NoAck:    false,
	}).Result()
	return c._returnXMessages(xstreams, err)
}

func (c *RedisClient) QueueAck(ctx context.Context, stream, group string, ids ...string) error {
	_, err := c.XAck(ctx, stream, group, ids...).Result()
	return err
}

type BatchIterator[T any] struct {
	ts   []T
	pos  int
	size int
}

func NewBatchIterator[T any](ts []T, size ...int) *BatchIterator[T] {
	s := ModelsBatchSize
	if len(size) > 0 && size[0] > 0 {
		s = size[0]
	}
	return &BatchIterator[T]{
		ts:   ts,
		pos:  -1,
		size: s,
	}
}

func (b *BatchIterator[T]) String() string {
	if b == nil {
		return "<nil>"
	}
	if b.pos < 0 {
		return fmt.Sprintf("BatchIt[-1](%d)", len(b.ts))
	}
	return fmt.Sprintf("BatchIt[%d:%d](%d)", b.pos, b.pos+b.size, len(b.ts))
}

func (b *BatchIterator[T]) HasNext() bool {
	if len(b.ts) == 0 {
		return false
	}
	if b.pos < 0 {
		return true
	}
	if (b.pos + b.size) >= len(b.ts) {
		return false
	}
	return true
}

func (b *BatchIterator[T]) Current() []T {
	if b.pos >= len(b.ts) {
		return nil
	}
	end := min(b.pos+b.size, len(b.ts))
	return b.ts[b.pos:end]
}

func (b *BatchIterator[T]) Next() []T {
	if len(b.ts) == 0 {
		return nil
	}
	if b.pos < 0 {
		b.pos = 0
	} else {
		b.pos += b.size
	}
	return b.Current()
}
