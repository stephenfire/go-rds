package rds

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

// RedisBatchValue 批量操作的泛型辅助结构体，封装了编码/解码器和批次大小，
// 用于将大量数据拆分为小批次进行 Redis 操作，避免单次操作数据量过大。
type RedisBatchValue[V any] struct {
	batchSize int // default consts.BatchSize
	encoder   RedisEncoder[V]
	decoder   RedisDecoder[V]
}

// _batchSize 返回批次大小，如果未设置则使用默认的 BatchSize。
func (b *RedisBatchValue[V]) _batchSize() int {
	if b.batchSize <= 0 {
		return BatchSize
	}
	return b.batchSize
}

// _batchValues 将数据按批次拆分，对每批数据编码后调用 op 执行操作。
// vs 为原始数据切片，op 为每批次执行的操作函数。
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

// _batchRun 与 _batchValues 类似，但 op 返回 int64 类型的计数结果，
// 最终将所有批次的返回值累加并返回总和。
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

// RedisList 是 Redis List 数据结构的泛型封装，支持批量操作。
// 通过组合 RedisBatchValue 实现自动分批编码/解码。
type RedisList[V any] struct {
	client redis.Cmdable
	RedisBatchValue[V]
}

func NewRedisList[V any](client redis.Cmdable, encoder RedisEncoder[V], decoder RedisDecoder[V]) *RedisList[V] {
	return &RedisList[V]{
		client:          client,
		RedisBatchValue: RedisBatchValue[V]{batchSize: 0, encoder: encoder, decoder: decoder},
	}
}

// WithBatchSize 设置批量操作时的批次大小，返回自身以支持链式调用。
func (l *RedisList[V]) WithBatchSize(size int) *RedisList[V] { l.batchSize = size; return l }

func pipeReturnLastCmd[C redis.Cmder, V any](
	ctx context.Context,
	client redis.Cmdable,
	batchRunner *RedisBatchValue[V],
	op func(cctx context.Context, pipe redis.Pipeliner, val ...any) C,
	vs ...V) (c C, err error) {
	var last C
	pipe := client.Pipeline()
	err = batchRunner._batchValues(func(vals ...interface{}) error {
		last = op(ctx, pipe, vals...)
		return nil
	}, vs...)
	if err != nil {
		return c, err
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return c, err
	}
	return last, nil
}

func (l *RedisList[V]) RPush(ctx context.Context, key string, vs ...V) (int64, error) {
	values, err := Encoder[V, string](l.encoder).EncodesAsAny(vs...)
	if err != nil {
		return 0, err
	}
	return l.client.RPush(ctx, key, values...).Result()
}

func (l *RedisList[V]) LPush(ctx context.Context, key string, vs ...V) (int64, error) {
	values, err := Encoder[V, string](l.encoder).EncodesAsAny(vs...)
	if err != nil {
		return 0, err
	}
	return l.client.LPush(ctx, key, values...).Result()
}

func (l *RedisList[V]) _pushInPipe(ctx context.Context,
	pushFn func(ctx context.Context, pipe redis.Pipeliner, vals ...any) *redis.IntCmd,
	vs ...V) (int64, error) {
	if len(vs) == 0 {
		return 0, nil
	}
	last, err := pipeReturnLastCmd[*redis.IntCmd](ctx, l.client, &l.RedisBatchValue, pushFn, vs...)
	if err != nil {
		return 0, err
	}
	if last == nil {
		return 0, errors.New("rds: last cmd is nil")
	}
	return last.Result()
}

// RPushInPipe 将数据从右侧推入 Redis List。支持批量推入，自动按批次大小拆分。
// 返回操作后列表的长度。
func (l *RedisList[V]) RPushInPipe(ctx context.Context, key string, vs ...V) (int64, error) {
	return l._pushInPipe(ctx, func(cctx context.Context, pipe redis.Pipeliner, val ...any) *redis.IntCmd {
		return pipe.RPush(cctx, key, val...)
	}, vs...)
}

func (l *RedisList[V]) LPushInPipe(ctx context.Context, key string, vs ...V) (int64, error) {
	return l._pushInPipe(ctx, func(ctx context.Context, pipe redis.Pipeliner, vals ...any) *redis.IntCmd {
		return pipe.LPush(ctx, key, vals...)
	}, vs...)
}

func (l *RedisList[V]) RPushX(ctx context.Context, key string, vs ...V) (int64, error) {
	values, err := Encoder[V, string](l.encoder).EncodesAsAny(vs...)
	if err != nil {
		return 0, err
	}
	return l.client.RPushX(ctx, key, values...).Result()
}

func (l *RedisList[V]) LPushX(ctx context.Context, key string, vs ...V) (int64, error) {
	values, err := Encoder[V, string](l.encoder).EncodesAsAny(vs...)
	if err != nil {
		return 0, err
	}
	return l.client.LPushX(ctx, key, values...).Result()
}

func (l *RedisList[V]) RPop(ctx context.Context, key string) (V, error) {
	return l.decoder.Decode(l.client.RPop(ctx, key).Result())
}

func (l *RedisList[V]) LPop(ctx context.Context, key string) (V, error) {
	return l.decoder.Decode(l.client.LPop(ctx, key).Result())
}

func (l *RedisList[V]) RPopCount(ctx context.Context, key string, count int) ([]V, error) {
	return l.decoder.Decodes(l.client.RPopCount(ctx, key, count).Result())
}

func (l *RedisList[V]) LPopCount(ctx context.Context, key string, count int) ([]V, error) {
	return l.decoder.Decodes(l.client.LPopCount(ctx, key, count).Result())
}

// LRange 获取 Redis List 中指定范围内的元素并解码为泛型类型。
// start 和 stop 为索引，支持负数索引（如 -1 表示最后一个元素）。
func (l *RedisList[V]) LRange(ctx context.Context, key string, start, stop int64) ([]V, error) {
	return l.decoder.Decodes(l.client.LRange(ctx, key, start, stop).Result())
}

func (l *RedisList[V]) LRem(ctx context.Context, key string, count int64, v V) (int64, error) {
	s, e := l.encoder(v)
	if e != nil {
		return 0, e
	}
	return l.client.LRem(ctx, key, count, s).Result()
}

func (l *RedisList[V]) LLen(ctx context.Context, key string) (int64, error) {
	return l.client.LLen(ctx, key).Result()
}
