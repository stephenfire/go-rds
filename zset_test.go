package rds

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-tools"
)

// ==================== 单元测试 ====================

func TestZB(t *testing.T) {
	tests := []struct {
		name string
		zb   *ZB
		min  string
		max  string
	}{
		{"score int", ZForScore().Val(0), "0", "0"},
		{"score int64", ZForScore().Val(int64(100)), "100", "100"},
		{"score int exclude", ZForScore().Val(50).Exclude(), "(50", "(50"},
		{"score int64 exclude", ZForScore().Val(int64(50)).Exclude(), "(50", "(50"},
		{"score infinite", ZForScore().Infinite(), "-inf", "+inf"},
		{"lex string", new(ZB).ForLex().Val("abc"), "[abc", "[abc"},
		{"lex string exclude", new(ZB).ForLex().Val("abc").Exclude(), "(abc", "(abc"},
		{"lex int", new(ZB).ForLex().Val(10), "[10", "[10"},
		{"lex infinite", new(ZB).ForLex().Infinite(), "-", "+"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.zb.Min(); got != test.min {
				t.Errorf("Min() want %q got %q", test.min, got)
			}
			if got := test.zb.Max(); got != test.max {
				t.Errorf("Max() want %q got %q", test.max, got)
			}
		})
	}

	// panic：score 边界只接受 int/int64，lex 边界只接受 int/int64/string
	expectPanic(t, func() { ZForScore().Val("abc").Min() })
	expectPanic(t, func() { new(ZB).ForLex().Val(1.5).Min() })
}

// expectPanic 断言 fn 会 panic
func expectPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("want panic, got none")
		}
	}()
	fn()
}

func TestZArgsMinMaxString(t *testing.T) {
	tests := []struct {
		name string
		args *ZArgs
		min  string
		max  string
	}{
		{"int", NewZArgs("k").Range(1, 2), "1", "2"},
		{"int64", NewZArgs("k").Range(int64(1), int64(2)), "1", "2"},
		{"string", NewZArgs("k").Range("a", "b"), "a", "b"},
		{"byScore int", NewZArgs("k").ByScore().Range(1, 2), "1", "2"},
		{"byScore zb exclude", NewZArgs("k").ByScore().Range(ZForScore().Val(1).Exclude(), ZForScore().Val(2)), "(1", "2"},
		{"byScore zb infinite", NewZArgs("k").ByScore().Range(ZForScore().Infinite(), ZForScore().Val(2)), "-inf", "2"},
		{"byLex zb", NewZArgs("k").ByLEX().Range(new(ZB).ForLex().Val("a"), new(ZB).ForLex().Val("z")), "[a", "[z"},
		{"byLex zb exclude", NewZArgs("k").ByLEX().Range(new(ZB).ForLex().Val("a").Exclude(), new(ZB).ForLex().Val("z")), "(a", "[z"},
		{"byLex string", NewZArgs("k").ByLEX().Range("[a", "[z"), "[a", "[z"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.args.MinString(); got != test.min {
				t.Errorf("MinString() want %q got %q", test.min, got)
			}
			if got := test.args.MaxString(); got != test.max {
				t.Errorf("MaxString() want %q got %q", test.max, got)
			}
		})
	}

	// panic：*ZB 不能作为 index 边界；不支持的类型
	expectPanic(t, func() { NewZArgs("k").Range(ZForScore().Val(1), 2).MinString() })
	expectPanic(t, func() { NewZArgs("k").Range(1.5, 2).MinString() })
	expectPanic(t, func() { NewZArgs("k").ByScore().Range(1.5, 2).MinString() })
}

func TestZArgsRedis(t *testing.T) {
	// nil receiver → 零值
	var nilArgs *ZArgs
	if got := nilArgs.Redis(); got != (redis.ZRangeArgs{}) {
		t.Errorf("nil ZArgs.Redis() want zero value, got %+v", got)
	}

	tests := []struct {
		name string
		args *ZArgs
		want redis.ZRangeArgs
	}{
		{
			"byIndex fw",
			NewZArgs("k").Range(0, -1),
			redis.ZRangeArgs{Key: "k", Start: "0", Stop: "-1"},
		},
		{
			// 按 index 的 REV 不交换（下标按反转后的集合解释）
			"byIndex rev no swap",
			NewZArgs("k").Range(0, 2).Rev(),
			redis.ZRangeArgs{Key: "k", Start: "0", Stop: "2", Rev: true},
		},
		{
			"byScore fw",
			NewZArgs("k").ByScore().Range(0, 100),
			redis.ZRangeArgs{Key: "k", Start: "0", Stop: "100", ByScore: true},
		},
		{
			// REV+BYSCORE 必须交换 min/max（go-redis v9.21.0 起不再自动交换）
			"byScore rev swap",
			NewZArgs("k").ByScore().Rev().Range(0, 100),
			redis.ZRangeArgs{Key: "k", Start: "100", Stop: "0", ByScore: true, Rev: true},
		},
		{
			"byLex rev swap",
			NewZArgs("k").ByLEX().Rev().Range(new(ZB).ForLex().Val("a"), new(ZB).ForLex().Val("z")),
			redis.ZRangeArgs{Key: "k", Start: "[z", Stop: "[a", ByLex: true, Rev: true},
		},
		{
			"paginate",
			NewZArgs("k").ByScore().Paginate(3, 7).Range(0, 100),
			redis.ZRangeArgs{Key: "k", Start: "0", Stop: "100", ByScore: true, Offset: 3, Count: 7},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.args.Redis(); got != test.want {
				t.Errorf("Redis() want %+v got %+v", test.want, got)
			}
		})
	}
}

func TestZArgsString(t *testing.T) {
	tests := []struct {
		name   string
		args   *ZArgs
		output string
	}{
		{"byIndex fw", NewZArgs("k").Range(0, -1), "k 0 -1"},
		{"byIndex rev", NewZArgs("k").Range(0, 2).Rev(), "k 0 2 REV"},
		{"byScore fw", NewZArgs("k").ByScore().Range(0, 100), "k 0 100 BYSCORE"},
		{"byScore rev swap", NewZArgs("k").ByScore().Rev().Range(0, 100), "k 100 0 BYSCORE REV"},
		{"byLex rev swap", NewZArgs("k").ByLEX().Rev().Range(new(ZB).ForLex().Val("a"), new(ZB).ForLex().Val("z")), "k [z [a BYLEX REV"},
		{"limit offset count", NewZArgs("k").ByScore().Paginate(1, 2).Range(0, 100), "k 0 100 BYSCORE LIMIT 1 2"},
		{"limit offset only", NewZArgs("k").Paginate(5, 0).Range(0, -1), "k 0 -1 LIMIT 5 0"},
		{"limit count only", NewZArgs("k").Paginate(0, 5).Range(0, -1), "k 0 -1 LIMIT 0 5"},
		{"no limit", NewZArgs("k").Paginate(0, 0).Range(0, -1), "k 0 -1"},
		{
			"byScore rev exclude",
			NewZArgs("zset_key").ByScore().Rev().Range(ZForScore().Val(0).Exclude(), ZForScore().Val(int64(1000000))),
			"zset_key 1000000 (0 BYSCORE REV",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.args.String(); got != test.output {
				t.Errorf("String() want %q got %q", test.output, got)
			}
		})
	}
}

func TestIdScore(t *testing.T) {
	// ScoreTime
	is := &IdScore{Id: 1, Score: 1700000000000}
	if got := is.ScoreTime().UnixMilli(); got != 1700000000000 {
		t.Errorf("ScoreTime().UnixMilli() want 1700000000000 got %d", got)
	}
	if got, want := is.ScoreTime(), tools.NewUnixTime(1700000000000); got.Compare(want) != 0 {
		t.Errorf("ScoreTime() want %v got %v", want, got)
	}

	// IdScoreToZ: nil → ok=false
	if z, ok, err := IdScoreToZ(nil); err != nil || ok || z != (redis.Z{}) {
		t.Errorf("IdScoreToZ(nil) want (zero, false, nil), got (%v, %v, %v)", z, ok, err)
	}
	// IdScoreToZ: 正常
	z, ok, err := IdScoreToZ(&IdScore{Id: 5, Score: 7})
	if err != nil || !ok {
		t.Fatalf("IdScoreToZ want ok, got (%v, %v, %v)", z, ok, err)
	}
	if z.Score != 7 || z.Member != "5" {
		t.Errorf("IdScoreToZ want {Score:7, Member:\"5\"}, got %+v", z)
	}

	// ZToIdScore: 往返
	got, err := ZToIdScore(redis.Z{Score: 7, Member: "5"})
	if err != nil {
		t.Fatalf("ZToIdScore: %v", err)
	}
	if got.Id != 5 || got.Score != 7 {
		t.Errorf("ZToIdScore want {Id:5, Score:7}, got %+v", got)
	}
	// ZToIdScore: member 非 string
	if _, err := ZToIdScore(redis.Z{Score: 7, Member: 5}); err != ErrExpectingString {
		t.Errorf("ZToIdScore(non-string member) want ErrExpectingString, got %v", err)
	}
	// ZToIdScore: member 非数字
	if _, err := ZToIdScore(redis.Z{Score: 7, Member: "abc"}); err == nil {
		t.Error("ZToIdScore(non-numeric member) want error, got nil")
	}
}

// ==================== 集成测试 ====================

// newTestIdScoreZSet 构造测试用 IdScore ZSet
func newTestIdScoreZSet(env *testRedisEnv, batchSize ...int) *RedisZSet[*IdScore] {
	return NewIdScoreZSet(env.client, batchSize...)
}

// newStringZSet 成员型 RedisZSet：Z 即成员字符串（分数统一为 0，仅用于成员序测试）
func newStringZSet(client redis.Cmdable) *RedisZSet[string] {
	return NewRedisZSet(client,
		func(s string) (redis.Z, bool, error) { return redis.Z{Score: 0, Member: s}, true, nil },
		func(z redis.Z) (string, error) {
			s, ok := z.Member.(string)
			if !ok {
				return "", ErrExpectingString
			}
			return s, nil
		})
}

// ids 便捷构造期望结果，score 恒为 id*10（与 zaddTestRangeData 的数据一致）
func ids(ids ...int64) []*IdScore {
	ret := make([]*IdScore, 0, len(ids))
	for _, id := range ids {
		ret = append(ret, &IdScore{Id: id, Score: id * 10})
	}
	return ret
}

// idsZero 便捷构造仅成员查询（ZRangeArgs）的期望结果，Score 恒为 0
func idsZero(ids ...int64) []*IdScore {
	ret := make([]*IdScore, 0, len(ids))
	for _, id := range ids {
		ret = append(ret, &IdScore{Id: id})
	}
	return ret
}

// assertIdScores 断言 IdScore 切片（元素顺序敏感，空与 nil 视为等价）
func assertIdScores(t *testing.T, want, got []*IdScore) {
	t.Helper()
	if len(want) == 0 && len(got) == 0 {
		return
	}
	if len(want) != len(got) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if *want[i] != *got[i] {
			t.Errorf("index %d want %+v got %+v", i, want[i], got[i])
		}
	}
}

// zaddTestRangeData 写入 5 个成员（id 1..5，score 10..50），供区间查询测试使用
func zaddTestRangeData(t *testing.T, env *testRedisEnv, key string) *RedisZSet[*IdScore] {
	t.Helper()
	zs := NewIdScoreZSet(env.client)
	if _, err := zs.ZAdd(context.Background(), key,
		&IdScore{Id: 1, Score: 10}, &IdScore{Id: 2, Score: 20}, &IdScore{Id: 3, Score: 30},
		&IdScore{Id: 4, Score: 40}, &IdScore{Id: 5, Score: 50}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	return zs
}

func TestZAdd(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "zadd")
	zs := newTestIdScoreZSet(env)

	// 空 vs → 0，且不创建 key
	if n, err := zs.ZAdd(ctx, key); err != nil || n != 0 {
		t.Fatalf("ZAdd(empty) want (0, nil), got (%d, %v)", n, err)
	}
	if card, _ := zs.ZCard(ctx, key); card != 0 {
		t.Errorf("ZCard want 0 got %d", card)
	}

	// 新增 3 个
	if n, err := zs.ZAdd(ctx, key, &IdScore{Id: 1, Score: 10}, &IdScore{Id: 2, Score: 20}, &IdScore{Id: 3, Score: 30}); err != nil || n != 3 {
		t.Fatalf("ZAdd(3) want (3, nil), got (%d, %v)", n, err)
	}

	// 重复添加改分：计数 0，分数已更新
	if n, err := zs.ZAdd(ctx, key, &IdScore{Id: 1, Score: 100}); err != nil || n != 0 {
		t.Fatalf("ZAdd(dup) want (0, nil), got (%d, %v)", n, err)
	}
	got, err := zs.ZRangeWithScores(ctx, NewZArgs(key).ByScore().Range(100, 100))
	if err != nil || len(got) != 1 || got[0].Id != 1 || got[0].Score != 100 {
		t.Errorf("ZAdd(dup) score not updated: got %+v, err %v", got, err)
	}

	// 分批写入：batchSize 2，写 5 个
	key2 := env.key(t, "zadd-batch")
	zs2 := newTestIdScoreZSet(env, 2)
	if n, err := zs2.ZAdd(ctx, key2,
		&IdScore{Id: 1, Score: 10}, &IdScore{Id: 2, Score: 20}, &IdScore{Id: 3, Score: 30},
		&IdScore{Id: 4, Score: 40}, &IdScore{Id: 5, Score: 50}); err != nil || n != 5 {
		t.Fatalf("ZAdd(batch) want (5, nil), got (%d, %v)", n, err)
	}
	if card, _ := zs2.ZCard(ctx, key2); card != 5 {
		t.Errorf("ZCard want 5 got %d", card)
	}

	// 错误类型 key → error
	key3 := env.key(t, "zadd-wrongtype")
	if err := env.client.Set(ctx, key3, "not-a-zset", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := zs.ZAdd(ctx, key3, &IdScore{Id: 1, Score: 10}); err == nil {
		t.Error("ZAdd on string key want error, got nil")
	}
}

func TestZAddNX(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "zaddnx")
	zs := newTestIdScoreZSet(env)

	// 空 vs → 0
	if n, err := zs.ZAddNX(ctx, key); err != nil || n != 0 {
		t.Fatalf("ZAddNX(empty) want (0, nil), got (%d, %v)", n, err)
	}
	// 新增 2 个
	if n, err := zs.ZAddNX(ctx, key, &IdScore{Id: 1, Score: 10}, &IdScore{Id: 2, Score: 20}); err != nil || n != 2 {
		t.Fatalf("ZAddNX(2) want (2, nil), got (%d, %v)", n, err)
	}
	// 已存在的成员不覆盖、不计入
	if n, err := zs.ZAddNX(ctx, key, &IdScore{Id: 1, Score: 99}, &IdScore{Id: 3, Score: 30}); err != nil || n != 1 {
		t.Fatalf("ZAddNX(1 new) want (1, nil), got (%d, %v)", n, err)
	}
	got, err := zs.ZRangeWithScores(ctx, NewZArgs(key).ByScore().Range(10, 10))
	if err != nil || len(got) != 1 || got[0].Id != 1 || got[0].Score != 10 {
		t.Errorf("ZAddNX overwrote existing: got %+v, err %v", got, err)
	}
	if card, _ := zs.ZCard(ctx, key); card != 3 {
		t.Errorf("ZCard want 3 got %d", card)
	}
}

func TestZAddWithEmpty(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	zs := newTestIdScoreZSet(env)
	empty := &IdScore{Id: 0, Score: -1} // 对应 EmptyZ {Score: -1, Member: "0"}

	// 空 vs → 写入 emptyValue
	key := env.key(t, "zadd-empty")
	if n, err := zs.ZAddWithEmpty(ctx, key, empty); err != nil || n != 1 {
		t.Fatalf("ZAddWithEmpty(empty) want (1, nil), got (%d, %v)", n, err)
	}
	got, err := zs.ZRangeWithScores(ctx, NewZArgs(key).Range(0, -1))
	if err != nil || len(got) != 1 || got[0].Id != 0 || got[0].Score != -1 {
		t.Errorf("ZAddWithEmpty want [{0, -1}], got %+v, err %v", got, err)
	}

	// 非空 vs → 不自动补 emptyValue
	key2 := env.key(t, "zadd-noempty")
	if n, err := zs.ZAddWithEmpty(ctx, key2, empty, &IdScore{Id: 1, Score: 10}); err != nil || n != 1 {
		t.Fatalf("ZAddWithEmpty(1) want (1, nil), got (%d, %v)", n, err)
	}
	if card, _ := zs.ZCard(ctx, key2); card != 1 {
		t.Errorf("ZCard want 1 got %d", card)
	}
}

func TestZAddNXWithEmpty(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	zs := newTestIdScoreZSet(env)
	empty := &IdScore{Id: 0, Score: -1}

	// 空 vs → 写入 emptyValue
	key := env.key(t, "zaddnx-empty")
	if n, err := zs.ZAddNXWithEmpty(ctx, key, empty); err != nil || n != 1 {
		t.Fatalf("ZAddNXWithEmpty(empty) want (1, nil), got (%d, %v)", n, err)
	}
	// 空值已存在 → NX 语义不覆盖
	if n, err := zs.ZAddNXWithEmpty(ctx, key, empty); err != nil || n != 0 {
		t.Fatalf("ZAddNXWithEmpty(dup) want (0, nil), got (%d, %v)", n, err)
	}
	if card, _ := zs.ZCard(ctx, key); card != 1 {
		t.Errorf("ZCard want 1 got %d", card)
	}
}

func TestZCard(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "zcard")
	zs := newTestIdScoreZSet(env)

	// 不存在的 key → 0
	if card, err := zs.ZCard(ctx, key); err != nil || card != 0 {
		t.Fatalf("ZCard(missing) want (0, nil), got (%d, %v)", card, err)
	}
	if _, err := zs.ZAdd(ctx, key, &IdScore{Id: 1, Score: 10}, &IdScore{Id: 2, Score: 20}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	if card, err := zs.ZCard(ctx, key); err != nil || card != 2 {
		t.Fatalf("ZCard want (2, nil), got (%d, %v)", card, err)
	}
}

func TestZPopMin(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "zpopmin")
	zs := newTestIdScoreZSet(env)

	// 不存在的 key → nil, nil
	if got, err := zs.ZPopMin(ctx, key); err != nil || got != nil {
		t.Fatalf("ZPopMin(missing) want (nil, nil), got (%v, %v)", got, err)
	}

	if _, err := zs.ZAdd(ctx, key, &IdScore{Id: 1, Score: 10}, &IdScore{Id: 2, Score: 20}, &IdScore{Id: 3, Score: 30}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	// 不指定 count → 弹最小 1 个
	got, err := zs.ZPopMin(ctx, key)
	if err != nil || len(got) != 1 || got[0].Id != 1 || got[0].Score != 10 {
		t.Fatalf("ZPopMin(1) want [{1 10}], got %+v, err %v", got, err)
	}

	// count=2 → 升序弹 2 个
	got, err = zs.ZPopMin(ctx, key, 2)
	if err != nil || len(got) != 2 || got[0].Id != 2 || got[1].Id != 3 {
		t.Fatalf("ZPopMin(2) want [{2 20} {3 30}], got %+v, err %v", got, err)
	}
	if card, _ := zs.ZCard(ctx, key); card != 0 {
		t.Errorf("ZCard want 0 got %d", card)
	}

	// count 超过大小 → 弹全部剩余
	if _, err := zs.ZAdd(ctx, key, &IdScore{Id: 1, Score: 10}, &IdScore{Id: 2, Score: 20}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	got, err = zs.ZPopMin(ctx, key, 5)
	if err != nil || len(got) != 2 {
		t.Fatalf("ZPopMin(5) want 2 elements, got %+v, err %v", got, err)
	}
}

func TestBZPopMin(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "bzpopmin")
	zs := newTestIdScoreZSet(env)

	// 空 key：阻塞至超时 → nil, nil
	got, err := zs.BZPopMin(ctx, time.Second, key)
	if err != nil || got != nil {
		t.Fatalf("BZPopMin(timeout) want (nil, nil), got (%v, %v)", got, err)
	}

	if _, err := zs.ZAdd(ctx, key, &IdScore{Id: 1, Score: 10}, &IdScore{Id: 2, Score: 20}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	// 有数据 → 弹最小 1 个
	got, err = zs.BZPopMin(ctx, time.Second, key)
	if err != nil || len(got) != 1 || got[0].Id != 1 || got[0].Score != 10 {
		t.Fatalf("BZPopMin want [{1 10}], got %+v, err %v", got, err)
	}
	if card, _ := zs.ZCard(ctx, key); card != 1 {
		t.Errorf("ZCard want 1 got %d", card)
	}
}

func TestZRangeWithScores(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "zrange")
	zs := zaddTestRangeData(t, env, key)

	tests := []struct {
		name string
		args *ZArgs
		want []*IdScore
	}{
		// 按 index
		{"index full", NewZArgs(key).Range(0, -1), ids(1, 2, 3, 4, 5)},
		{"index slice", NewZArgs(key).Range(1, 2), ids(2, 3)},
		{"index negative", NewZArgs(key).Range(-1, -1), ids(5)},
		// REV 按下标反转后的集合解释：0=最高分，-1=最低分
		{"index rev full", NewZArgs(key).Range(0, -1).Rev(), ids(5, 4, 3, 2, 1)},
		{"index rev top2", NewZArgs(key).Range(0, 1).Rev(), ids(5, 4)},
		{"index rev negative", NewZArgs(key).Range(-1, -1).Rev(), ids(1)},
		{"index out of range", NewZArgs(key).Range(10, 20), nil},
		// 按 score
		{"score inclusive", NewZArgs(key).ByScore().Range(20, 40), ids(2, 3, 4)},
		{"score exclude min", NewZArgs(key).ByScore().Range(ZForScore().Val(20).Exclude(), ZForScore().Val(40)), ids(3, 4)},
		{"score exclude max", NewZArgs(key).ByScore().Range(ZForScore().Val(20), ZForScore().Val(40).Exclude()), ids(2, 3)},
		{"score both excluded", NewZArgs(key).ByScore().Range(ZForScore().Val(20).Exclude(), ZForScore().Val(40).Exclude()), ids(3)},
		{"score infinite min", NewZArgs(key).ByScore().Range(ZForScore().Infinite(), ZForScore().Val(30)), ids(1, 2, 3)},
		{"score infinite max", NewZArgs(key).ByScore().Range(ZForScore().Val(40), ZForScore().Infinite()), ids(4, 5)},
		{"score int bounds", NewZArgs(key).ByScore().Range(20, 40), ids(2, 3, 4)},
		{"score string bounds", NewZArgs(key).ByScore().Range("20", "40"), ids(2, 3, 4)},
		{"score empty range", NewZArgs(key).ByScore().Range(100, 200), nil},
		// 按 score + REV（REV 时 min/max 交换的修复验证）
		{"score rev", NewZArgs(key).ByScore().Rev().Range(20, 40), ids(4, 3, 2)},
		{"score rev exclude", NewZArgs(key).ByScore().Rev().Range(ZForScore().Val(20).Exclude(), ZForScore().Val(40)), ids(4, 3)},
		{"score rev infinite", NewZArgs(key).ByScore().Rev().Range(ZForScore().Val(20), ZForScore().Infinite()), ids(5, 4, 3, 2)},
		// LIMIT
		{"limit offset0 count2", NewZArgs(key).ByScore().Paginate(0, 2).Range(0, 100), ids(1, 2)},
		{"limit offset1 count2", NewZArgs(key).ByScore().Paginate(1, 2).Range(0, 100), ids(2, 3)},
		{"limit 0 0 all", NewZArgs(key).ByScore().Paginate(0, 0).Range(0, 100), ids(1, 2, 3, 4, 5)},
		{"limit count5", NewZArgs(key).ByScore().Paginate(0, 5).Range(0, 100), ids(1, 2, 3, 4, 5)},
		{"limit beyond", NewZArgs(key).ByScore().Paginate(10, 2).Range(0, 100), nil},
		// 组合：score + REV + LIMIT
		{"score rev limit", NewZArgs(key).ByScore().Rev().Paginate(1, 2).Range(0, 100), ids(4, 3)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := zs.ZRangeWithScores(ctx, test.args)
			if err != nil {
				t.Fatalf("ZRangeWithScores: %v", err)
			}
			assertIdScores(t, test.want, got)
		})
	}

	// 不存在的 key → nil, nil
	missingKey := env.key(t, "zrange-missing")
	if got, err := zs.ZRangeWithScores(ctx, NewZArgs(missingKey).Range(0, -1)); err != nil || got != nil {
		t.Errorf("missing key want (nil, nil), got (%v, %v)", got, err)
	}

	// 错误类型 key → error
	wrongKey := env.key(t, "zrange-wrongtype")
	if err := env.client.Set(ctx, wrongKey, "not-a-zset", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := zs.ZRangeWithScores(ctx, NewZArgs(wrongKey).Range(0, -1)); err == nil {
		t.Error("ZRangeWithScores on string key want error, got nil")
	}

	// BYLEX + WITHSCORES：Redis 不支持该组合（syntax error），lex 区间应使用 ZRangeArgs
	byLexArgs := NewZArgs(key).ByLEX().Range("[1", "[5")
	if _, err := zs.ZRangeWithScores(ctx, byLexArgs); err == nil {
		t.Error("ZRangeWithScores+BYLEX want error (Redis 不支持 WITHSCORES 与 BYLEX 组合), got nil")
	}
}

func TestZRangeArgs(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "zrangeargs")
	zs := zaddTestRangeData(t, env, key)

	tests := []struct {
		name string
		args *ZArgs
		want []*IdScore // 仅成员查询，Score 恒为 0
	}{
		{"index full", NewZArgs(key).Range(0, -1), idsZero(1, 2, 3, 4, 5)},
		{"index rev", NewZArgs(key).Range(0, -1).Rev(), idsZero(5, 4, 3, 2, 1)},
		{"score fw", NewZArgs(key).ByScore().Range(20, 40), idsZero(2, 3, 4)},
		{"score rev", NewZArgs(key).ByScore().Rev().Range(20, 40), idsZero(4, 3, 2)},
		{"score exclude limit", NewZArgs(key).ByScore().Paginate(1, 2).Range(ZForScore().Val(10).Exclude(), ZForScore().Val(50).Exclude()), idsZero(3, 4)},
		{"score empty range", NewZArgs(key).ByScore().Range(100, 200), nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := zs.ZRangeArgs(ctx, test.args)
			if err != nil {
				t.Fatalf("ZRangeArgs: %v", err)
			}
			assertIdScores(t, test.want, got)
		})
	}

	// 不存在的 key → nil, nil
	missingKey := env.key(t, "zrangeargs-missing")
	if got, err := zs.ZRangeArgs(ctx, NewZArgs(missingKey).Range(0, -1)); err != nil || got != nil {
		t.Errorf("missing key want (nil, nil), got (%v, %v)", got, err)
	}

	// 错误类型 key → error
	wrongKey := env.key(t, "zrangeargs-wrongtype")
	if err := env.client.Set(ctx, wrongKey, "not-a-zset", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := zs.ZRangeArgs(ctx, NewZArgs(wrongKey).Range(0, -1)); err == nil {
		t.Error("ZRangeArgs on string key want error, got nil")
	}

	// BYLEX：成员 1、2、10 的 lex 序为 1 < 10 < 2。
	// 注意 BYLEX 要求所有成员分数相同（分数不同时结果仍按 score 排序），这里统一用分数 1
	lexKey := env.key(t, "zrangeargs-lex")
	zsLex := NewIdScoreZSet(env.client)
	if _, err := zsLex.ZAdd(ctx, lexKey, &IdScore{Id: 1, Score: 1}, &IdScore{Id: 2, Score: 1}, &IdScore{Id: 10, Score: 1}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	lexTests := []struct {
		name string
		args *ZArgs
		want []int64
	}{
		{"lex fw", NewZArgs(lexKey).ByLEX().Range("[1", "[2"), []int64{1, 10, 2}},
		{"lex exclude min", NewZArgs(lexKey).ByLEX().Range("(1", "[2"), []int64{10, 2}},
		{"lex infinite", NewZArgs(lexKey).ByLEX().Range(new(ZB).ForLex().Infinite(), new(ZB).ForLex().Val("2")), []int64{1, 10, 2}},
		{"lex rev", NewZArgs(lexKey).ByLEX().Rev().Range("[1", "[2"), []int64{2, 10, 1}},
		{"lex rev limit", NewZArgs(lexKey).ByLEX().Rev().Paginate(1, 1).Range("[1", "[2"), []int64{10}},
	}
	for _, test := range lexTests {
		t.Run(test.name, func(t *testing.T) {
			got, err := zsLex.ZRangeArgs(ctx, test.args)
			if err != nil {
				t.Fatalf("ZRangeArgs: %v", err)
			}
			want := idsZero(test.want...)
			assertIdScores(t, want, got)
		})
	}

	// 成员型 RedisZSet 的 BYLEX 场景
	sKey := env.key(t, "zrangeargs-string")
	sset := newStringZSet(env.client)
	if _, err := sset.ZAdd(ctx, sKey, "apple", "banana", "cherry", "date"); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	got, err := sset.ZRangeArgs(ctx, NewZArgs(sKey).ByLEX().Range("[b", "[d"))
	if err != nil {
		t.Fatalf("ZRangeArgs: %v", err)
	}
	if want := []string{"banana", "cherry"}; !reflect.DeepEqual(want, got) {
		t.Errorf("BYLEX [b [d want %v, got %v", want, got)
	}
}

func TestZCount(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "zcount")
	zs := zaddTestRangeData(t, env, key) // score 10,20,30,40,50

	tests := []struct {
		min, max string
		want     int64
	}{
		{"10", "30", 3},
		{"(10", "30", 2},
		{"10", "(30", 2},
		{"(10", "(30", 1},
		{"100", "200", 0},
		{"-inf", "+inf", 5},
	}
	for _, test := range tests {
		if got, err := zs.ZCount(ctx, key, test.min, test.max); err != nil || got != test.want {
			t.Errorf("ZCount(%s, %s) want (%d, nil), got (%d, %v)", test.min, test.max, test.want, got, err)
		}
	}
	// 不存在的 key → 0
	if got, err := zs.ZCount(ctx, env.key(t, "zcount-missing"), "-inf", "+inf"); err != nil || got != 0 {
		t.Errorf("ZCount(missing) want (0, nil), got (%d, %v)", got, err)
	}
}

func TestZAddCall(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "zaddcall")
	zs := newTestIdScoreZSet(env)

	// 自定义 op（等价 ZAdd），走 _batchAdd 管线
	if n, err := zs.ZAddCall(func(client redis.Cmdable, vals ...redis.Z) (int64, error) {
		return client.ZAdd(ctx, key, vals...).Result()
	}, &IdScore{Id: 1, Score: 10}, &IdScore{Id: 2, Score: 20}); err != nil || n != 2 {
		t.Fatalf("ZAddCall want (2, nil), got (%d, %v)", n, err)
	}
	// 分批
	zs2 := newTestIdScoreZSet(env, 2)
	if n, err := zs2.ZAddCall(func(client redis.Cmdable, vals ...redis.Z) (int64, error) {
		return client.ZAdd(ctx, key, vals...).Result()
	}, &IdScore{Id: 3, Score: 30}, &IdScore{Id: 4, Score: 40}, &IdScore{Id: 5, Score: 50}); err != nil || n != 3 {
		t.Fatalf("ZAddCall(batch) want (3, nil), got (%d, %v)", n, err)
	}
	if card, _ := zs2.ZCard(ctx, key); card != 5 {
		t.Errorf("ZCard want 5 got %d", card)
	}
}
