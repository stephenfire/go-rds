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
	// IdScore represents a zset element: an int64 member ID paired with an int64 score.
	IdScore struct {
		Id    int64
		Score int64
	}

	// ZB is a boundary value for SCORE or LEX ranges used by ZRange.
	// Use ForScore/ForLex to pick the range mode and Val/Infinite to set the value.
	ZB struct {
		V        any
		AsScore  bool // true for score range, false for lexicographical range
		Excluded bool
	}

	// ZArgs describes a ZRANGE query: key, range bounds, ordering mode, direction
	// and pagination. Build it fluently, e.g. NewZArgs(key).ByScore().Rev().Range(min, max).
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

	// RedisZSet is a generic Redis sorted-set wrapper for values of type Z, where Z
	// carries both the member and the score. Use NewIdScoreZSet for the common
	// {Id, Score} case, or NewRedisZSet with custom codecs.
	RedisZSet[Z any] struct {
		client    redis.Cmdable
		batchSize int
		encoder   RedisZEncoder[Z]
		decoder   RedisZDecoder[Z]
	}
)

// ZForScore returns a score-mode ZB boundary builder.
func ZForScore() *ZB {
	return new(ZB).ForScore()
}

// Val sets the boundary value. Use nil (or Infinite) for an open end.
func (b *ZB) Val(v any) *ZB { b.V = v; return b }

// Infinite makes the boundary open-ended: -inf for Min, +inf for Max.
func (b *ZB) Infinite() *ZB { b.V = nil; return b }

// Exclude marks the boundary as excluded from the range.
func (b *ZB) Exclude() *ZB { b.Excluded = true; return b }

// Include marks the boundary as included in the range (the default).
func (b *ZB) Include() *ZB { b.Excluded = false; return b }

// ForScore switches the boundary to score mode.
func (b *ZB) ForScore() *ZB { b.AsScore = true; return b }

// ForLex switches the boundary to lexicographical mode.
func (b *ZB) ForLex() *ZB { b.AsScore = false; return b }

// Min renders the boundary as a range minimum.
func (b *ZB) Min() string { return b.Boundary(true) }

// Max renders the boundary as a range maximum.
func (b *ZB) Max() string { return b.Boundary(false) }

// Boundary renders the boundary for a Redis command. isMin selects the open-ended
// default: -inf/- for a minimum, +inf/+ for a maximum. Panics on unsupported value types.
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

// Range sets the range bounds. min is always the smaller value and max the larger
// one; with REV the effective interval on the wire becomes (max, min).
func (a *ZArgs) Range(min, max any) *ZArgs { a.min, a.max = min, max; return a }

// Rev sets reverse order (highest first). For REV with BYSCORE/BYLEX, pass the
// bounds in ascending order; they are swapped for the wire automatically.
func (a *ZArgs) Rev() *ZArgs { a.rev = true; return a }

// Fw sets forward order (lowest first). This is the default.
func (a *ZArgs) Fw() *ZArgs { a.rev = false; return a }

// ByIndex selects range by index (the default). With REV, indices are interpreted
// on the reversed set (0 = highest score).
func (a *ZArgs) ByIndex() *ZArgs { a.byScore, a.byLex = false, false; return a }

// ByScore selects range by score.
func (a *ZArgs) ByScore() *ZArgs { a.byScore, a.byLex = true, false; return a }

// ByLEX selects range by lexicographical order. Only meaningful when all members
// share the same score.
func (a *ZArgs) ByLEX() *ZArgs { a.byScore, a.byLex = false, true; return a }

// Paginate applies LIMIT offset count to the range result.
func (a *ZArgs) Paginate(begin, size int64) *ZArgs { a.offset, a.count = begin, size; return a }

// WithKey sets the zset key.
func (a *ZArgs) WithKey(key string) *ZArgs { a.key = key; return a }

// NewZArgs creates a ZArgs for the given key (index mode, forward by default).
func NewZArgs(key string) *ZArgs { return &ZArgs{key: key} }

// MinString renders the range minimum as a Redis boundary string. Panics on
// unsupported boundary types.
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
		panic(errors.New("rds: ZB cannot be an index range boundary"))
	default:
		panic(fmt.Errorf("invalid type of start boundary: %s", reflect.TypeOf(a.min).String()))
	}
}

// MaxString renders the range maximum as a Redis boundary string. Panics on
// unsupported boundary types.
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
		panic(errors.New("rds: ZB cannot be an index range boundary"))
	default:
		panic(fmt.Errorf("invalid type of start boundary: %s", reflect.TypeOf(a.max).String()))
	}
}

// Redis converts the query into go-redis ZRangeArgs.
// Redis expects "ZRANGE key max min BYSCORE REV": with REV the min/max positions are
// swapped on the wire. go-redis v9.14.0 swapped them automatically in appendArgs, but
// v9.21.0 removed that and leaves it to the caller, so swap here to match String().
// Index ranges with REV need no swap (indices are interpreted on the reversed set).
func (a *ZArgs) Redis() redis.ZRangeArgs {
	if a == nil {
		return redis.ZRangeArgs{}
	}
	start := a.MinString()
	stop := a.MaxString()
	if a.rev && (a.byScore || a.byLex) {
		start, stop = stop, start
	}
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

// String renders the query as a command-like string for logging.
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
	if a.offset != 0 || a.count != 0 {
		b.WriteByte(' ')
		b.WriteString("LIMIT ")
		b.WriteString(fmt.Sprintf("%d %d", a.offset, a.count))
	}
	return b.String()
}

// ScoreTime interprets the score as a Unix timestamp.
func (i *IdScore) ScoreTime() tools.Time {
	return tools.NewUnixTime(i.Score)
}

// IdScoreToZ is the RedisZEncoder for *IdScore: Id becomes the member, Score the score.
func IdScoreToZ(is *IdScore) (redis.Z, bool, error) {
	if is == nil {
		return redis.Z{}, false, nil
	}
	return redis.Z{
		Score:  float64(is.Score),
		Member: tools.ID(is.Id).String(),
	}, true, nil
}

// ZToIdScore is the RedisZDecoder for *IdScore: parses the member back into the ID.
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

// NewIdScoreZSet creates a RedisZSet[*IdScore] with the built-in codecs.
func NewIdScoreZSet(client redis.Cmdable, batchSize ...int) *RedisZSet[*IdScore] {
	return NewRedisZSet[*IdScore](client, IdScoreToZ, ZToIdScore, batchSize...)
}

// NewRedisZSet creates a RedisZSet[Z] with custom encode/decode functions.
// encoder returns ok=false to skip a value. batchSize optionally overrides the
// write batch size.
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

// WithBatchSize sets the write batch size; values are sent in batches to avoid
// oversized commands.
func (zs *RedisZSet[Z]) WithBatchSize(size int) *RedisZSet[Z] { zs.batchSize = size; return zs }

// _batchSize returns the configured batch size, falling back to the global BatchSize.
func (zs *RedisZSet[Z]) _batchSize() int {
	return tools.IF(zs.batchSize <= 0, BatchSize, zs.batchSize)
}

// _batchValues encodes vs and invokes op per batch.
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

// _batchAdd sums the per-batch counts returned by op.
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

// ZAddCall runs a custom add op (e.g. ZAddNX) in batches over the encoded values.
func (zs *RedisZSet[Z]) ZAddCall(op func(client redis.Cmdable, vals ...redis.Z) (int64, error), vs ...Z) (int64, error) {
	return zs._batchAdd(func(vals ...redis.Z) (int64, error) {
		return op(zs.client, vals...)
	}, vs...)
}

// ZAddNX adds values only for members not already present and returns the number
// of new members.
func (zs *RedisZSet[Z]) ZAddNX(ctx context.Context, key string, vs ...Z) (int64, error) {
	if len(vs) == 0 {
		return 0, nil
	}
	return zs._batchAdd(func(vals ...redis.Z) (int64, error) {
		return zs.client.ZAddNX(ctx, key, vals...).Result()
	}, vs...)
}

// ZAddNXWithEmpty behaves like ZAddNX but writes emptyValue when vs is empty.
func (zs *RedisZSet[Z]) ZAddNXWithEmpty(ctx context.Context, key string, emptyValue Z, vs ...Z) (int64, error) {
	if len(vs) == 0 {
		vs = append(vs, emptyValue)
	}
	return zs._batchAdd(func(vals ...redis.Z) (int64, error) {
		return zs.client.ZAddNX(ctx, key, vals...).Result()
	}, vs...)
}

// ZAdd upserts values (existing members get their score updated) and returns the
// number of new members.
func (zs *RedisZSet[Z]) ZAdd(ctx context.Context, key string, vs ...Z) (int64, error) {
	if len(vs) == 0 {
		return 0, nil
	}
	return zs._batchAdd(func(vals ...redis.Z) (int64, error) {
		return zs.client.ZAdd(ctx, key, vals...).Result()
	}, vs...)
}

// ZAddWithEmpty behaves like ZAdd but writes emptyValue when vs is empty.
func (zs *RedisZSet[Z]) ZAddWithEmpty(ctx context.Context, key string, emptyValue Z, vs ...Z) (int64, error) {
	if len(vs) == 0 {
		vs = append(vs, emptyValue)
	}
	return zs._batchAdd(func(vals ...redis.Z) (int64, error) {
		return zs.client.ZAdd(ctx, key, vals...).Result()
	}, vs...)
}

// ZCard returns the number of members in the zset.
func (zs *RedisZSet[Z]) ZCard(ctx context.Context, key string) (int64, error) {
	return zs.client.ZCard(ctx, key).Result()
}

// _parseZs decodes a redis.Z slice, converting redis.Nil into (nil, nil).
func (zs *RedisZSet[Z]) _parseZs(redisZs []redis.Z, err error) ([]Z, error) {
	if err != nil {
		if IsRedisNil(err) {
			return nil, nil
		}
		return nil, err
	}
	return Zs[Z](redisZs).Decode(zs.decoder)
}

// ZPopMin removes and returns up to count members with the lowest scores
// (default 1), in ascending order.
func (zs *RedisZSet[Z]) ZPopMin(ctx context.Context, key string, count ...int64) ([]Z, error) {
	return zs._parseZs(zs.client.ZPopMin(ctx, key, count...).Result())
}

// BZPopMin blocks up to timeout for the member with the lowest score. The redis.Nil
// timeout error is converted to a nil slice, so nil means "no result".
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

// ZRangeWithScores returns the full Z entries (member + score) in the range; the
// ordering is controlled by ZArgs (ByIndex/ByScore/ByLEX).
// Note: Redis does not support WITHSCORES combined with BYLEX; use ZRangeArgs for
// lex ranges.
func (zs *RedisZSet[Z]) ZRangeWithScores(ctx context.Context, args *ZArgs) ([]Z, error) {
	vs, err := zs.client.ZRangeArgsWithScores(ctx, args.Redis()).Result()
	if err != nil {
		return nil, err
	}
	return Zs[Z](vs).Decode(zs.decoder)
}

// ZRangeArgs returns the members in the range without scores, mirroring
// RedisMSZSet.ZRangeArgs.
// Note: decoded Z values only carry the member; Score is 0. Use ZRangeWithScores
// when scores are needed.
func (zs *RedisZSet[Z]) ZRangeArgs(ctx context.Context, args *ZArgs) ([]Z, error) {
	members, err := zs.client.ZRangeArgs(ctx, args.Redis()).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}
	ret := make([]Z, 0, len(members))
	for _, member := range members {
		v, err := zs.decoder(redis.Z{Member: member})
		if err != nil {
			return nil, err
		}
		ret = append(ret, v)
	}
	return ret, nil
}

// ZCount counts the members with scores in [min, max]. Bounds are inclusive by
// default; prefix a bound with "(" to exclude it.
func (zs *RedisZSet[Z]) ZCount(ctx context.Context, key string, min, max string) (int64, error) {
	return zs.client.ZCount(ctx, key, min, max).Result()
}
