package rds

import (
	"context"
	"reflect"

	"github.com/stephenfire/go-tools"
)

type (
	// IsNil 判断泛型值是否为空/零值的函数类型。
	IsNil[T any] func(T) bool

	// RedisTree 是 Redis 二级树形数据结构的泛型封装。
	// 第一级 key（K1）映射到第二级 key 列表（[]K2），第二级 key（K2）映射到实际数据（V）。
	// 适用于父子关系数据的缓存场景，如分类-商品、用户-订单等。
	RedisTree[K1 comparable, K2 comparable, V any] struct {
		parent       *RedisString[K1, []K2] // 父级缓存：K1 -> []K2
		child        *RedisString[K2, V]    // 子级缓存：K2 -> V
		parentLoader MapLoader[K1, []K2]    // 父级数据加载器
		childLoader  MapLoader[K2, V]       // 子级数据加载器
		isChildNil   IsNil[V]               // 判断子级值是否为空
	}
)

// IsDefaultZero 判断泛型值 t 是否为其类型的默认零值（如 int 的 0、string 的 ""、指针的 nil）。
func IsDefaultZero[T any](t T) bool {
	val := reflect.ValueOf(t)
	if !val.IsValid() {
		return true
	}
	return val.IsZero()
}

// NewRedisTree 创建一个新的 RedisTree 实例。
// isChildNil 为可选参数，用于自定义判断子级值是否为空的逻辑；
// 如果不提供，则默认使用 IsDefaultZero 判断。
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

// _isChildNil 判断子级值是否为空。如果设置了自定义的 isChildNil 函数则使用它，
// 否则使用 IsDefaultZero 进行默认零值判断。
func (t *RedisTree[K1, K2, V]) _isChildNil(v V) bool {
	if t.isChildNil == nil {
		return IsDefaultZero(v)
	}
	return t.isChildNil(v)
}

// Children 根据父级 key 列表获取对应的子级数据。
// 先通过 parent 缓存获取每个 K1 对应的 K2 列表，再通过 child 缓存获取每个 K2 对应的实际数据 V，
// 最终返回 map[K1][]V 的映射结果。空值子数据会被自动过滤。
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
