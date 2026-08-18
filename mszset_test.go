package rds

import (
	"context"
	"testing"
)

// newTestIDZSet 构造测试用 ID ZSet（成员与分数均为 int64）
func newTestIDZSet(env *testRedisEnv) *RedisMSZSet[int64, int64] {
	return NewIDZSet(env.client)
}

func TestMS(t *testing.T) {
	ms := MS[int64, int64]{M: 5, S: 7}
	if ms.Key() != 5 || ms.Value() != 7 {
		t.Errorf("MS want {Key:5, Value:7}, got %+v", ms)
	}
}

func TestMSZSetZAdd(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "mszadd")
	ms := newTestIDZSet(env)

	if n, err := ms.ZAdd(ctx, key, map[int64]int64{1: 10, 2: 20, 3: 30}); err != nil || n != 3 {
		t.Fatalf("ZAdd want (3, nil), got (%d, %v)", n, err)
	}
	// 重复添加：计数 0，分数更新
	if n, err := ms.ZAdd(ctx, key, map[int64]int64{1: 100}); err != nil || n != 0 {
		t.Fatalf("ZAdd(dup) want (0, nil), got (%d, %v)", n, err)
	}
	if score, err := ms.ZScore(ctx, key, 1); err != nil || score != 100 {
		t.Fatalf("ZScore want (100, nil), got (%d, %v)", score, err)
	}
	// 分批：batchSize 2，写 5 个
	key2 := env.key(t, "mszadd-batch")
	ms2 := NewIDZSet(env.client).WithBatchSize(2)
	if n, err := ms2.ZAdd(ctx, key2, map[int64]int64{1: 10, 2: 20, 3: 30, 4: 40, 5: 50}); err != nil || n != 5 {
		t.Fatalf("ZAdd(batch) want (5, nil), got (%d, %v)", n, err)
	}
	if count, _ := ms2.ZCount(ctx, key2, "-inf", "+inf"); count != 5 {
		t.Errorf("ZCount want 5 got %d", count)
	}
}

func TestMSZSetZScoreZMScoreZIncrBy(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "mszscore")
	ms := newTestIDZSet(env)

	if _, err := ms.ZAdd(ctx, key, map[int64]int64{1: 10, 2: 20, 3: 30}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	// ZScore: 存在的成员
	if score, err := ms.ZScore(ctx, key, 2); err != nil || score != 20 {
		t.Fatalf("ZScore want (20, nil), got (%d, %v)", score, err)
	}
	// ZScore: 不存在的成员 → redis.Nil
	if _, err := ms.ZScore(ctx, key, 99); !IsRedisNil(err) {
		t.Errorf("ZScore(missing) want redis.Nil, got %v", err)
	}

	// ZIncrBy: 加 5 → 15
	if score, err := ms.ZIncrBy(ctx, key, 5, 1); err != nil || score != 15 {
		t.Fatalf("ZIncrBy want (15, nil), got (%d, %v)", score, err)
	}
	if score, err := ms.ZScore(ctx, key, 1); err != nil || score != 15 {
		t.Fatalf("ZScore after ZIncrBy want (15, nil), got (%d, %v)", score, err)
	}

	// ZMScore: 批量取分
	scores, err := ms.ZMScore(ctx, key, 1, 2, 3)
	if err != nil {
		t.Fatalf("ZMScore: %v", err)
	}
	want := map[int64]int64{1: 15, 2: 20, 3: 30}
	if len(scores) != len(want) {
		t.Fatalf("ZMScore want %v, got %v", want, scores)
	}
	for k, v := range want {
		if scores[k] != v {
			t.Errorf("ZMScore[%d] want %d got %d", k, v, scores[k])
		}
	}
	// ZMScore: 重复成员去重
	if scores, err := ms.ZMScore(ctx, key, 1, 1, 2); err != nil || len(scores) != 2 {
		t.Errorf("ZMScore(dup) want 2 entries, got %v, err %v", scores, err)
	}
}

func TestMSZSetZRangeArgs(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "mszrangeargs")
	ms := newTestIDZSet(env)

	// 0 作为占位成员
	if _, err := ms.ZAdd(ctx, key, map[int64]int64{0: 0, 1: 10, 2: 20, 3: 30}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	tests := []struct {
		name        string
		args        *ZArgs
		placeHolder []int64
		want        []int64
	}{
		{"index full", NewZArgs(key).Range(0, -1), nil, []int64{0, 1, 2, 3}},
		{"index rev", NewZArgs(key).Range(0, -1).Rev(), nil, []int64{3, 2, 1, 0}},
		{"score fw", NewZArgs(key).ByScore().Range(10, 30), nil, []int64{1, 2, 3}},
		{"score rev", NewZArgs(key).ByScore().Rev().Range(10, 30), nil, []int64{3, 2, 1}},
		{"score exclude limit", NewZArgs(key).ByScore().Paginate(1, 2).Range(ZForScore().Val(0).Exclude(), ZForScore().Val(30)), nil, []int64{2, 3}},
		{"placeholder filter", NewZArgs(key).Range(0, -1), []int64{0}, []int64{1, 2, 3}},
		{"empty range", NewZArgs(key).ByScore().Range(100, 200), nil, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ms.ZRangeArgs(ctx, test.args, test.placeHolder...)
			if err != nil {
				t.Fatalf("ZRangeArgs: %v", err)
			}
			if len(test.want) == 0 && len(got) == 0 {
				return
			}
			if len(test.want) != len(got) {
				t.Fatalf("want %v, got %v", test.want, got)
			}
			for i := range test.want {
				if test.want[i] != got[i] {
					t.Errorf("index %d want %d got %d", i, test.want[i], got[i])
				}
			}
		})
	}

	// 不存在的 key → nil, nil
	missingKey := env.key(t, "mszrangeargs-missing")
	if got, err := ms.ZRangeArgs(ctx, NewZArgs(missingKey).Range(0, -1)); err != nil || got != nil {
		t.Errorf("missing key want (nil, nil), got (%v, %v)", got, err)
	}

	// BYLEX：成员 1、2、10 的 lex 序为 1 < 10 < 2。
	// 注意 BYLEX 要求所有成员分数相同，这里统一用分数 1
	lexKey := env.key(t, "mszrangeargs-lex")
	msLex := NewIDZSet(env.client)
	if _, err := msLex.ZAdd(ctx, lexKey, map[int64]int64{1: 1, 2: 1, 10: 1}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}
	if got, err := msLex.ZRangeArgs(ctx, NewZArgs(lexKey).ByLEX().Range("[1", "[2")); err != nil {
		t.Fatalf("ZRangeArgs: %v", err)
	} else if want := []int64{1, 10, 2}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("BYLEX [1 [2 want %v, got %v", want, got)
	}
}

func TestMSZSetZRangeArgsWithScores(t *testing.T) {
	env := newTestRedis(t)
	ctx := context.Background()
	key := env.key(t, "mszrangeargsscores")
	ms := newTestIDZSet(env)

	// 0 作为占位成员
	if _, err := ms.ZAdd(ctx, key, map[int64]int64{0: 0, 1: 10, 2: 20, 3: 30}); err != nil {
		t.Fatalf("ZAdd: %v", err)
	}

	tests := []struct {
		name        string
		args        *ZArgs
		placeHolder []int64
		want        []MS[int64, int64]
	}{
		{"index full", NewZArgs(key).Range(0, -1), nil, []MS[int64, int64]{{0, 0}, {1, 10}, {2, 20}, {3, 30}}},
		{"score fw", NewZArgs(key).ByScore().Range(10, 30), nil, []MS[int64, int64]{{1, 10}, {2, 20}, {3, 30}}},
		{"score rev", NewZArgs(key).ByScore().Rev().Range(10, 30), nil, []MS[int64, int64]{{3, 30}, {2, 20}, {1, 10}}},
		{"score exclude limit", NewZArgs(key).ByScore().Paginate(1, 2).Range(ZForScore().Val(0).Exclude(), ZForScore().Val(30)), nil, []MS[int64, int64]{{2, 20}, {3, 30}}},
		{"placeholder filter", NewZArgs(key).Range(0, -1), []int64{0}, []MS[int64, int64]{{1, 10}, {2, 20}, {3, 30}}},
		{"empty range", NewZArgs(key).ByScore().Range(100, 200), nil, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ms.ZRangeArgsWithScores(ctx, test.args, test.placeHolder...)
			if err != nil {
				t.Fatalf("ZRangeArgsWithScores: %v", err)
			}
			if len(test.want) == 0 && len(got) == 0 {
				return
			}
			if len(test.want) != len(got) {
				t.Fatalf("want %v, got %v", test.want, got)
			}
			for i := range test.want {
				if test.want[i] != got[i] {
					t.Errorf("index %d want %+v got %+v", i, test.want[i], got[i])
				}
			}
		})
	}

	// 不存在的 key → nil, nil
	missingKey := env.key(t, "mszrangeargsscores-missing")
	if got, err := ms.ZRangeArgsWithScores(ctx, NewZArgs(missingKey).Range(0, -1)); err != nil || got != nil {
		t.Errorf("missing key want (nil, nil), got (%v, %v)", got, err)
	}

	// BYLEX + WITHSCORES：Redis 不支持该组合（syntax error），lex 区间只能使用 ZRangeArgs
	if _, err := ms.ZRangeArgsWithScores(ctx, NewZArgs(key).ByLEX().Range("[1", "[3")); err == nil {
		t.Error("ZRangeArgsWithScores+BYLEX want error (Redis 不支持 WITHSCORES 与 BYLEX 组合), got nil")
	}

	// KeyValuer 互操作：MS 实现 KeyValuer，可经 ConvertKV 转换
	scoreGot, err := ms.ZRangeArgsWithScores(ctx, NewZArgs(key).ByScore().Range(10, 30))
	if err != nil {
		t.Fatalf("ZRangeArgsWithScores: %v", err)
	}
	kv := ConvertKV(scoreGot)
	if len(kv) != 3 || kv[0].Key() != 1 || kv[0].Value() != 10 || kv[2].Key() != 3 || kv[2].Value() != 30 {
		t.Errorf("ConvertKV want [{1 10} {2 20} {3 30}], got %+v", scoreGot)
	}
}
