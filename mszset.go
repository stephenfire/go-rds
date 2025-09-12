package rds

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-tools"
)

type RedisMSZSet[M comparable, S any] struct {
	client        redis.Cmdable
	memberEncoder RedisEncoder[M]
	memberDecoder RedisDecoder[M]
	scoreEncoder  RedisFloatEncoder[S]
	scoreDecoder  RedisFloatDecoder[S]
}

func NewMSZSet[M comparable, S any](client redis.Cmdable, memberEncoder RedisEncoder[M], memberDecoder RedisDecoder[M],
	scoreEncoder RedisFloatEncoder[S], scoreDecoder RedisFloatDecoder[S]) *RedisMSZSet[M, S] {
	return &RedisMSZSet[M, S]{
		client:        client,
		memberEncoder: memberEncoder,
		memberDecoder: memberDecoder,
		scoreEncoder:  scoreEncoder,
		scoreDecoder:  scoreDecoder,
	}
}

func NewIDZSet(client redis.Cmdable) *RedisMSZSet[int64, int64] {
	return NewMSZSet(client, RedisInt64Encoder, RedisInt64Decoder, Int64ToFloat64, Float64ToInt64)
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

func (ms *RedisMSZSet[M, S]) ZRangeArgs(ctx context.Context, args *ZArgs) ([]M, error) {
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
			ret = append(ret, k)
		}
	}
	return ret, nil
}

func (ms *RedisMSZSet[M, S]) ZCount(ctx context.Context, key string, min, max string) (count int64, err error) {
	return ms.client.ZCount(ctx, key, min, max).Result()
}
