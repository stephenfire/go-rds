package rds

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-tools"
)

type (
	IdScore struct {
		Id    int64
		Score int64
	}

	// ZB boundary for SCORE or LEX range for ZRange
	ZB struct {
		V        any
		AsScore  bool // true for score range, false for lexicographical range
		Excluded bool
	}

	ZArgs struct {
		key     string
		min     any
		max     any
		byScore bool
		byLex   bool
		rev     bool
		offset  int64
		count   int64
	}

	RedisZSet[Z any] struct {
		client    redis.Cmdable
		batchSize int
		encoder   RedisZEncoder[Z]
		decoder   RedisZDecoder[Z]
	}
)

func ZForScore() *ZB {
	return new(ZB).ForScore()
}

func (b *ZB) Val(v any) *ZB { b.V = v; return b }
func (b *ZB) Infinite() *ZB { b.V = nil; return b }
func (b *ZB) Exclude() *ZB  { b.Excluded = true; return b }
func (b *ZB) Include() *ZB  { b.Excluded = false; return b }
func (b *ZB) ForScore() *ZB { b.AsScore = true; return b }
func (b *ZB) ForLex() *ZB   { b.AsScore = false; return b }
func (b *ZB) Min() string   { return b.Boundary(true) }
func (b *ZB) Max() string   { return b.Boundary(false) }
func (b *ZB) Boundary(isMin bool) string {
	if b.AsScore {
		if b.V == nil {
			return tools.IF(isMin, "-inf", "+inf")
		}
		switch v := b.V.(type) {
		case int64, int:
			if b.Excluded {
				return fmt.Sprintf("(%d", v)
			}
			return fmt.Sprintf("%d", v)
		default:
			panic(fmt.Errorf("invalid type of score boundary: %s", reflect.TypeOf(b.V).String()))
		}
	} else {
		if b.V == nil {
			return tools.IF(isMin, "-", "+")
		}
		sym := tools.IF(b.Excluded, "(", "[")
		switch v := b.V.(type) {
		case int64, int:
			return fmt.Sprintf("%s%d", sym, v)
		case string:
			return sym + v
		default:
			panic(fmt.Errorf("invalid type of lex boundary: %s", reflect.TypeOf(b.V).String()))
		}
	}
}

// Range min 永远是小值，max 是大值，当REV为true时，区间为 (max, min)
func (a *ZArgs) Range(min, max any) *ZArgs         { a.min, a.max = min, max; return a }
func (a *ZArgs) Rev() *ZArgs                       { a.rev = true; return a }
func (a *ZArgs) Fw() *ZArgs                        { a.rev = false; return a }
func (a *ZArgs) ByIndex() *ZArgs                   { a.byScore, a.byLex = false, false; return a }
func (a *ZArgs) ByScore() *ZArgs                   { a.byScore, a.byLex = true, false; return a }
func (a *ZArgs) ByLEX() *ZArgs                     { a.byScore, a.byLex = false, true; return a }
func (a *ZArgs) Paginate(begin, size int64) *ZArgs { a.offset, a.count = begin, size; return a }
func (a *ZArgs) WithKey(key string) *ZArgs         { a.key = key; return a }
func NewZArgs(key string) *ZArgs                   { return &ZArgs{key: key} }

func (a *ZArgs) MinString() string {
	switch v := a.min.(type) {
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case string:
		return v
	case *ZB:
		if a.byScore {
			return v.ForScore().Min()
		} else if a.byLex {
			return v.ForLex().Min()
		}
		panic(errors.New("ZB cannot be an index range boundary"))
	default:
		panic(fmt.Errorf("invalid type of start boundary: %s", reflect.TypeOf(a.min).String()))
	}
}

func (a *ZArgs) MaxString() string {
	switch v := a.max.(type) {
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case string:
		return v
	case *ZB:
		if a.byScore {
			return v.ForScore().Max()
		} else if a.byLex {
			return v.ForLex().Max()
		}
		panic(errors.New("ZB cannot be an index range boundary"))
	default:
		panic(fmt.Errorf("invalid type of start boundary: %s", reflect.TypeOf(a.max).String()))
	}
}

func (a *ZArgs) Redis() redis.ZRangeArgs {
	if a == nil {
		return redis.ZRangeArgs{}
	}
	start := a.MinString()
	stop := a.MaxString()
	// 在redis中，"ZRANGE key min max BYSCORE REV"时，min和max实际上正好相反，而这个问题又被go-redis库调换回来了，所以这里直接填写
	// if a.rev && (a.byScore || a.byLex) {
	// 	start, stop = stop, start
	// }
	return redis.ZRangeArgs{
		Key:     a.key,
		Start:   start,
		Stop:    stop,
		ByScore: a.byScore,
		ByLex:   a.byLex,
		Rev:     a.rev,
		Offset:  a.offset,
		Count:   a.count,
	}
}

func (a *ZArgs) String() string {
	var b strings.Builder
	b.WriteString(a.key)
	if a.rev && (a.byScore || a.byLex) {
		b.WriteByte(' ')
		b.WriteString(a.MaxString())
		b.WriteByte(' ')
		b.WriteString(a.MinString())
	} else {
		b.WriteByte(' ')
		b.WriteString(a.MinString())
		b.WriteByte(' ')
		b.WriteString(a.MaxString())
	}
	if a.byScore {
		b.WriteByte(' ')
		b.WriteString("BYSCORE")
	} else if a.byLex {
		b.WriteByte(' ')
		b.WriteString("BYLEX")
	}
	if a.rev {
		b.WriteByte(' ')
		b.WriteString("REV")
	}
	if a.offset != 0 && a.count != 0 {
		b.WriteByte(' ')
		b.WriteString("LIMIT ")
		b.WriteString(fmt.Sprintf("%d %d", a.offset, a.count))
	}
	return b.String()
}

func (i *IdScore) ScoreTime() tools.Time {
	return tools.NewUnixTime(i.Score)
}

func IdScoreToZ(is *IdScore) (redis.Z, bool, error) {
	if is == nil {
		return redis.Z{}, false, nil
	}
	return redis.Z{
		Score:  float64(is.Score),
		Member: tools.ID(is.Id).String(),
	}, true, nil
}

func ZToIdScore(z redis.Z) (*IdScore, error) {
	tz := Z(z)
	id, err := tz.KeyID()
	if err != nil {
		return nil, err
	}
	return &IdScore{
		Id:    id.Int(),
		Score: tz.ScoreInt(),
	}, nil
}

func NewIdScoreZSet(client redis.Cmdable, batchSize ...int) *RedisZSet[*IdScore] {
	return NewRedisZSet[*IdScore](client, IdScoreToZ, ZToIdScore, batchSize...)
}

func NewRedisZSet[Z any](client redis.Cmdable, encoder RedisZEncoder[Z],
	decoder RedisZDecoder[Z], batchSize ...int) *RedisZSet[Z] {
	size := 0
	if len(batchSize) > 0 && batchSize[0] > 0 {
		size = batchSize[0]
	}
	return &RedisZSet[Z]{
		client:    client,
		batchSize: size,
		encoder:   encoder,
		decoder:   decoder,
	}
}

func (zs *RedisZSet[Z]) WithBatchSize(size int) *RedisZSet[Z] { zs.batchSize = size; return zs }

func (zs *RedisZSet[Z]) _batchSize() int {
	return tools.IF(zs.batchSize <= 0, BatchSize, zs.batchSize)
}

func (zs *RedisZSet[Z]) _batchValues(op func(vals ...redis.Z) error, vs ...Z) error {
	batchSize := zs._batchSize()
	batchIt := NewBatchIterator(vs, batchSize)
	values := make([]redis.Z, 0, min(len(vs), batchSize))
	var z redis.Z
	var ok bool
	var err error
	for batchIt.HasNext() {
		values = values[:0]
		bvs := batchIt.Next()
		for _, bv := range bvs {
			z, ok, err = zs.encoder(bv)
			if err != nil {
				return err
			}
			if ok {
				values = append(values, z)
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

func (zs *RedisZSet[Z]) _batchAdd(op func(vals ...redis.Z) (int64, error), vs ...Z) (int64, error) {
	var sum, n int64
	var err error
	errr := zs._batchValues(func(vals ...redis.Z) error {
		n, err = op(vals...)
		sum += n
		return err
	}, vs...)
	return sum, errr
}

func (zs *RedisZSet[Z]) ZAddCall(op func(client redis.Cmdable, vals ...redis.Z) (int64, error), vs ...Z) (int64, error) {
	return zs._batchAdd(func(vals ...redis.Z) (int64, error) {
		return op(zs.client, vals...)
	}, vs...)
}

func (zs *RedisZSet[Z]) ZAddNX(ctx context.Context, key string, vs ...Z) (int64, error) {
	if len(vs) == 0 {
		return 0, nil
	}
	return zs._batchAdd(func(vals ...redis.Z) (int64, error) {
		return zs.client.ZAddNX(ctx, key, vals...).Result()
	}, vs...)
}

func (zs *RedisZSet[Z]) ZAddNXWithEmpty(ctx context.Context, key string, emptyValue Z, vs ...Z) (int64, error) {
	if len(vs) == 0 {
		vs = append(vs, emptyValue)
	}
	return zs._batchAdd(func(vals ...redis.Z) (int64, error) {
		return zs.client.ZAddNX(ctx, key, vals...).Result()
	}, vs...)
}

func (zs *RedisZSet[Z]) ZAdd(ctx context.Context, key string, vs ...Z) (int64, error) {
	if len(vs) == 0 {
		return 0, nil
	}
	return zs._batchAdd(func(vals ...redis.Z) (int64, error) {
		return zs.client.ZAdd(ctx, key, vals...).Result()
	}, vs...)
}

func (zs *RedisZSet[Z]) ZAddWithEmpty(ctx context.Context, key string, emptyValue Z, vs ...Z) (int64, error) {
	if len(vs) == 0 {
		vs = append(vs, emptyValue)
	}
	return zs._batchAdd(func(vals ...redis.Z) (int64, error) {
		return zs.client.ZAdd(ctx, key, vals...).Result()
	}, vs...)
}

func (zs *RedisZSet[Z]) ZCard(ctx context.Context, key string) (int64, error) {
	return zs.client.ZCard(ctx, key).Result()
}

func (zs *RedisZSet[Z]) _parseZs(redisZs []redis.Z, err error) ([]Z, error) {
	if err != nil {
		if IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	return Zs[Z](redisZs).Decode(zs.decoder)
}

func (zs *RedisZSet[Z]) ZPopMin(ctx context.Context, key string, count ...int64) ([]Z, error) {
	return zs._parseZs(zs.client.ZPopMin(ctx, key, count...).Result())
}

// BZPopMin 为了体现zset中没有数据，超时返回时的redis.Nil错误，统一返回slice，可以用nil表示没有返回值的情况
func (zs *RedisZSet[Z]) BZPopMin(ctx context.Context, timeout time.Duration, key string) ([]Z, error) {
	zwithKey, err := zs.client.BZPopMin(ctx, timeout, key).Result()
	if err != nil {
		if IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	return Zs[Z]([]redis.Z{zwithKey.Z}).Decode(zs.decoder)
}

func (zs *RedisZSet[Z]) ZRangeWithScores(ctx context.Context, args *ZArgs) ([]Z, error) {
	vs, err := zs.client.ZRangeArgsWithScores(ctx, args.Redis()).Result()
	if err != nil {
		return nil, err
	}
	return Zs[Z](vs).Decode(zs.decoder)
}

func (zs *RedisZSet[Z]) ZCount(ctx context.Context, key string, min, max string) (int64, error) {
	return zs.client.ZCount(ctx, key, min, max).Result()
}
