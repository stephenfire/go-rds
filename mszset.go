package rds

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-tools"
)

// RedisMSZSet is a generic Redis sorted-set wrapper that keeps members (M) and
// scores (S) as separate first-class types: scores can be read and updated
// independently via ZScore/ZMScore/ZIncrBy. Use NewIDZSet for the common
// int64/int64 case, or NewMSZSet with custom codecs.
type RedisMSZSet[M comparable, S any] struct {
	client        redis.Cmdable
	batchSize     int
	memberEncoder RedisEncoder[M]
	memberDecoder RedisDecoder[M]
	scoreEncoder  RedisFloatEncoder[S]
	scoreDecoder  RedisFloatDecoder[S]
}

// MS is an ordered member-score pair returned by ZRangeArgsWithScores.
// It implements KeyValuer, so it can be converted with ConvertKV to reuse the
// KeyValuers iterators.
type MS[M comparable, S any] struct {
	M M
	S S
}

// Key returns the member (KeyValuer implementation).
func (ms MS[M, S]) Key() M { return ms.M }

// Value returns the score (KeyValuer implementation).
func (ms MS[M, S]) Value() S { return ms.S }

// NewMSZSet creates a RedisMSZSet with custom member/score codecs. batchSize
// optionally overrides the write batch size.
func NewMSZSet[M comparable, S any](client redis.Cmdable, memberEncoder RedisEncoder[M], memberDecoder RedisDecoder[M],
	scoreEncoder RedisFloatEncoder[S], scoreDecoder RedisFloatDecoder[S], batchSize ...int) *RedisMSZSet[M, S] {
	size := 0
	if len(batchSize) > 0 && batchSize[0] > 0 {
		size = batchSize[0]
	}
	return &RedisMSZSet[M, S]{
		client:        client,
		batchSize:     size,
		memberEncoder: memberEncoder,
		memberDecoder: memberDecoder,
		scoreEncoder:  scoreEncoder,
		scoreDecoder:  scoreDecoder,
	}
}

// NewIDZSet creates a RedisMSZSet[int64, int64]: both members and scores are int64.
func NewIDZSet(client redis.Cmdable) *RedisMSZSet[int64, int64] {
	return NewMSZSet(client, RedisInt64Encoder, RedisInt64Decoder, Int64ToFloat64, Float64ToInt64)
}

// WithBatchSize sets the write batch size.
func (ms *RedisMSZSet[M, S]) WithBatchSize(batchSize int) *RedisMSZSet[M, S] {
	ms.batchSize = batchSize
	return ms
}

// _batchSize returns the configured batch size, falling back to the global BatchSize.
func (ms *RedisMSZSet[M, S]) _batchSize() int {
	return tools.IF(ms.batchSize <= 0, BatchSize, ms.batchSize)
}

// _batchAdd encodes msmap and invokes op per batch, summing the counts.
func (ms *RedisMSZSet[M, S]) _batchAdd(op func(vals ...redis.Z) (int64, error), msmap map[M]S) (int64, error) {
	batchSize := ms._batchSize()
	sum := int64(0)
	var zs []redis.Z
	for m, s := range msmap {
		z, err := ms._toZ(m, s)
		if err != nil {
			return sum, err
		}
		zs = append(zs, z)
		if len(zs) >= batchSize {
			n, err := op(zs...)
			if err != nil {
				return sum, err
			}
			sum += n
			zs = zs[:0]
		}
	}
	if len(zs) > 0 {
		n, err := op(zs...)
		if err != nil {
			return sum, err
		}
		sum += n
	}
	return sum, nil
}

// _toZ encodes a member-score pair into a redis.Z.
func (ms *RedisMSZSet[M, S]) _toZ(m M, s S) (redis.Z, error) {
	member, err := ms.memberEncoder(m)
	if err != nil {
		return redis.Z{}, err
	}
	score, err := ms.scoreEncoder(s)
	if err != nil {
		return redis.Z{}, err
	}
	return redis.Z{
		Score:  score,
		Member: member,
	}, nil
}

// ZScore returns the score of member m; redis.Nil when the member is missing.
func (ms *RedisMSZSet[M, S]) ZScore(ctx context.Context, key string, m M) (s S, err error) {
	member, errr := ms.memberEncoder(m)
	if errr != nil {
		err = errr
		return
	}
	redisScore, errr := ms.client.ZScore(ctx, key, member).Result()
	if errr != nil {
		err = errr
		return
	}
	return ms.scoreDecoder(redisScore)
}

// ZMScore returns the scores of the given members (deduplicated) as a map.
// Members whose scores fail to decode are skipped.
func (ms *RedisMSZSet[M, S]) ZMScore(ctx context.Context, key string, inputs ...M) (map[M]S, error) {
	ks := tools.KS[M](inputs).Dedup().Slice()
	if len(ks) == 0 {
		return nil, nil
	}
	members := make([]string, 0, len(ks))
	for _, k := range ks {
		if s, err := ms.memberEncoder(k); err != nil {
			return nil, err
		} else {
			members = append(members, s)
		}
	}
	redisScores, err := ms.client.ZMScore(ctx, key, members...).Result()
	if err != nil {
		return nil, err
	}
	if len(redisScores) != len(members) {
		return nil, errors.New("rds: redis zset.zmscore returned wrong number of results")
	}
	ret := make(map[M]S, len(redisScores))
	for i, redisScore := range redisScores {
		score, err := ms.scoreDecoder(redisScore)
		if err != nil {
			// unrecognized score
			continue
		}
		ret[ks[i]] = score
	}
	return ret, nil
}

// ZIncrBy increments the score of member m by incrBy and returns the new score.
func (ms *RedisMSZSet[M, S]) ZIncrBy(ctx context.Context, key string, incrBy float64, m M) (s S, err error) {
	member, errr := ms.memberEncoder(m)
	if errr != nil {
		err = errr
		return
	}
	redisScore, errr := ms.client.ZIncrBy(ctx, key, incrBy, member).Result()
	if errr != nil {
		err = errr
		return
	}
	return ms.scoreDecoder(redisScore)
}

// ZRangeArgs returns the members in the range without scores. placeHolder
// optionally filters placeholder members from the result.
func (ms *RedisMSZSet[M, S]) ZRangeArgs(ctx context.Context, args *ZArgs, placeHolder ...M) ([]M, error) {
	members, err := ms.client.ZRangeArgs(ctx, args.Redis()).Result()
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, nil
	}
	ret := make([]M, 0, len(members))
	for _, member := range members {
		if k, err := ms.memberDecoder(member); err != nil {
			return nil, err
		} else {
			if len(placeHolder) > 0 && placeHolder[0] == k {
				continue
			}
			ret = append(ret, k)
		}
	}
	return ret, nil
}

// ZRangeArgsWithScores returns the ordered member-score pairs (MS[M, S]) in the
// range. placeHolder filters placeholder members like in ZRangeArgs.
// Note: Redis does not support WITHSCORES combined with BYLEX; use ZRangeArgs for
// lex ranges.
func (ms *RedisMSZSet[M, S]) ZRangeArgsWithScores(ctx context.Context, args *ZArgs, placeHolder ...M) ([]MS[M, S], error) {
	zs, err := ms.client.ZRangeArgsWithScores(ctx, args.Redis()).Result()
	if err != nil {
		return nil, err
	}
	if len(zs) == 0 {
		return nil, nil
	}
	ret := make([]MS[M, S], 0, len(zs))
	for _, z := range zs {
		memberStr, ok := z.Member.(string)
		if !ok {
			return nil, ErrExpectingString
		}
		member, err := ms.memberDecoder(memberStr)
		if err != nil {
			return nil, err
		}
		if len(placeHolder) > 0 && placeHolder[0] == member {
			continue
		}
		score, err := ms.scoreDecoder(z.Score)
		if err != nil {
			return nil, err
		}
		ret = append(ret, MS[M, S]{M: member, S: score})
	}
	return ret, nil
}

// ZCount counts the members with scores in [min, max]. Bounds are inclusive by
// default; prefix a bound with "(" to exclude it.
func (ms *RedisMSZSet[M, S]) ZCount(ctx context.Context, key string, min, max string) (count int64, err error) {
	return ms.client.ZCount(ctx, key, min, max).Result()
}

// ZAdd upserts the members with their scores (existing members get their score
// updated) and returns the number of new members.
func (ms *RedisMSZSet[M, S]) ZAdd(ctx context.Context, key string, msmap map[M]S) (int64, error) {
	return ms._batchAdd(func(vals ...redis.Z) (int64, error) {
		return ms.client.ZAdd(ctx, key, vals...).Result()
	}, msmap)
}
