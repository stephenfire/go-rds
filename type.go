package rds

import "context"

type (
	// MapLoader 是一个从 Redis 加载数据的回调函数类型。
	// 接收一组 key，返回 key-value 映射，用于批量回源加载缺失数据。
	MapLoader[K comparable, V any] func(ctx context.Context, ks ...K) (map[K]V, error)
)
