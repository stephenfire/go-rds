package rds

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-tools"
)

type RedisMSZSet[M comparable, S any] struct {
	client        redis.Cmdable
	batchSize     int
	memberEncoder RedisEncoder[M]
	memberDecoder RedisDecoder[M]
	scoreEncoder  RedisFloatEncoder[S]
	scoreDecoder  RedisFloatDecoder[S]
}

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

func NewIDZSet(client redis.Cmdable) *RedisMSZSet[int64, int64] {
	return NewMSZSet(client, RedisInt64Encoder, RedisInt64Decoder, Int64ToFloat64, Float64ToInt64)
}

func (ms *RedisMSZSet[M, S]) WithBatchSize(batchSize int) *RedisMSZSet[M, S] {
	ms.batchSize = batchSize
	return ms
}

func (ms *RedisMSZSet[M, S]) _batchSize() int {
	return tools.IF(ms.batchSize <= 0, BatchSize, ms.batchSize)
}

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
		return nil, errors.New("redis zset.zmscore returned wrong number of results")
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

func (ms *RedisMSZSet[M, S]) ZCount(ctx context.Context, key string, min, max string) (count int64, err error) {
	return ms.client.ZCount(ctx, key, min, max).Result()
}

func (ms *RedisMSZSet[M, S]) ZAdd(ctx context.Context, key string, msmap map[M]S) (int64, error) {
	return ms._batchAdd(func(vals ...redis.Z) (int64, error) {
		return ms.client.ZAdd(ctx, key, vals...).Result()
	}, msmap)
}
