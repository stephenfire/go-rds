package rds

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/redis/go-redis/v9"
)

// 集成测试使用本机真实 Redis（默认 127.0.0.1:6379 DB 15，可用环境变量覆盖）：
//
//	REDIS_ADDR     redis 地址，默认 127.0.0.1:6379
//	REDIS_TEST_DB  测试用 db 编号，默认 15
//
// 所有测试 key 都带唯一前缀并在测试结束时删除，不会 FLUSHDB，不影响库中其他数据。
const testKeyPrefix = "go-rds:test:zset:"

type testRedisEnv struct {
	client *redis.Client
	keys   []string
}

func newTestRedis(t *testing.T) *testRedisEnv {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	db := 15
	if v := os.Getenv("REDIS_TEST_DB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("invalid REDIS_TEST_DB: %v", err)
		}
		db = n
	}
	client := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis unreachable at %s db %d (start redis or set REDIS_ADDR/REDIS_TEST_DB): %v", addr, db, err)
	}
	env := &testRedisEnv{client: client}
	t.Cleanup(func() {
		env.delKeys()
		_ = client.Close()
	})
	return env
}

// key 生成测试专属 key 并登记，测试结束后统一 DEL
func (e *testRedisEnv) key(t *testing.T, suffix string) string {
	t.Helper()
	key := fmt.Sprintf("%s%s:%s", testKeyPrefix, t.Name(), suffix)
	e.keys = append(e.keys, key)
	return key
}

func (e *testRedisEnv) delKeys() {
	if len(e.keys) == 0 {
		return
	}
	_ = e.client.Del(context.Background(), e.keys...).Err()
}
