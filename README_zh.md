# go-rds

一个高级的 Go Redis 客户端库，提供类型安全操作、批处理功能和增强的 Redis 数据结构支持。

## 特性

- **类型安全操作**: 支持各种数据类型的自动编码/解码
- **批处理**: 可配置批处理大小的高效批操作
- **Redis 数据结构**: 全面支持字符串、哈希、列表、集合和有序集合
- **分布式锁**: 内置 Redis 锁实现
- **流支持**: Redis Stream 操作，用于消息队列
- **缓存穿透保护**: 防止缓存穿透的机制
- **连接管理**: 带版本检测和连接池的 Redis 客户端

## 安装

```bash
go get github.com/stephenfire/go-rds
```

## 快速开始

### 基本用法

```go
package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/stephenfire/go-rds"
)

func main() {
	ctx := context.Background()
	
	// 连接到 Redis
	client, err := rds.ConnectRedis(ctx, "redis://localhost:6379")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// 设置值
	err = client.Set(ctx, "mykey", "Hello, Redis!", 10*time.Minute)
	if err != nil {
		log.Fatal(err)
	}

	// 获取值
	value, exists, err := client.Get(ctx, "mykey", reflect.TypeOf(""))
	if err != nil {
		log.Fatal(err)
	}
	if exists {
		fmt.Println("值:", value.(string))
	}
}
```

### 字符串操作

```go
// 创建字符串读写器
stringRW := rds.NewRedisString[string, string](
	client.C(),
	rds.RedisStringEncoder,
	rds.RedisStringDecoder,
	rds.RedisStringEncoder,
	rds.RedisStringDecoder,
)

// 设置多个值
err := stringRW.Sets(ctx, map[string]string{
	"key1": "value1",
	"key2": "value2",
	"key3": "value3",
})

// 获取多个值
notCached, cached, err := stringRW.Gets(ctx, "key1", "key2", "key3")
```

### 哈希操作

```go
// 创建哈希读写器
hasher := rds.NewRedisHasher[string, int64](
	client.C(),
	rds.RedisStringEncoder, rds.RedisStringDecoder,
	rds.RedisInt64Encoder, rds.RedisInt64Decoder,
)

// 设置哈希值
err := hasher.HSetsMap(ctx, "user:scores", map[string]int64{
	"user1": 100,
	"user2": 200,
	"user3": 300,
})

// 获取哈希值
notCached, cached, err := hasher.HGets(ctx, "user:scores", "user1", "user2")
```

### 有序集合操作

```go
// 创建 ID 分数的有序集合
zset := rds.NewIdScoreZSet(client.C())

// 向有序集合添加分数
scores := []*rds.IdScore{
	{Id: 1, Score: 100},
	{Id: 2, Score: 200},
	{Id: 3, Score: 300},
}

count, err := zset.ZAdd(ctx, "leaderboard", scores...)

// 查询前10名分数
args := &rds.ZArgs{}
args.WithKey("leaderboard").ByScore().Range(0, 1000).Paginate(0, 10).Rev()
topScores, err := zset.ZRangeWithScores(ctx, args)
```

### 分布式锁

```go
// 创建 Redis 锁
locker := redislock.New(client.C())
redisLock := rds.NewRedisLock(client.C(), locker, "my-lock", "lock-value", 30*time.Second)

// 获取锁
lockingValue, err := redisLock.Fetch(ctx)
if err != nil {
	log.Fatal("获取锁失败")
}

// 执行关键工作
fmt.Println("锁已获取，执行关键工作...")

// 释放锁
err = redisLock.Release()
if err != nil {
	log.Fatal("释放锁失败")
}
```

### 流操作

```go
// 初始化流组
err := client.InitStreamGroup(ctx, "mystream", "mygroup")

// 推送消息到流
messages := []map[string]interface{}{
	{"field1": "value1", "field2": 123},
	{"field1": "value2", "field2": 456},
}

err = client.QueuePushMaxLen(ctx, "mystream", 1000, messages...)

// 从流读取消息
msgs, err := client.QueueReadGroup(ctx, "mystream", "mygroup", "worker1", 5*time.Second, 10)
```

## 配置

### 连接选项

```go
// 自定义连接，指定超时时间
opts, err := redis.ParseURL("redis://localhost:6379")
if err != nil {
	log.Fatal(err)
}
opts.ReadTimeout = 2 * time.Second
opts.WriteTimeout = 5 * time.Second

client := redis.NewClient(opts)
redisClient := &rds.RedisClient{Client: client}
```

### 批处理大小配置

```go
// 为不同操作配置批处理大小
const (
	ModelsBatchSize = 100  // 哈希操作
	BatchSize       = 500  // 有序集合操作
)
```

## 高级用法

### 自定义类型编码/解码

```go
type User struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// 自定义编码器
func userEncoder(u User) (string, error) {
	return rds.RedisEncode(u)
}

// 自定义解码器  
func userDecoder(s string) (User, error) {
	var u User
	err := rds.RedisDecode(reflect.TypeOf(u), s, &u)
	return u, err
}

// 使用自定义类型
userHasher := rds.NewRedisHasher[int64, User](
	client.C(),
	rds.RedisInt64Encoder, rds.RedisInt64Decoder,
	userEncoder, userDecoder,
)
```

### 缓存穿透保护

```go
// 使用 HGetsAndSetsIfNA 防止缓存穿透
flagField := int64(-1) // 特殊标志字段
placeholderUser := User{ID: -1, Name: "", Email: ""}

users, err := userHasher.HGetsAndSetsIfNA(ctx, "users:cache", 
	func(ctx context.Context) (map[int64]User, error) {
		// 从数据库加载所有用户
		return loadAllUsersFromDB(ctx)
	}, 
	flagField, 
	userID1, userID2, userID3,
)
```

## 错误处理

```go
// 检查 Redis nil 错误
value, exists, err := client.Get(ctx, "nonexistent", reflect.TypeOf(""))
if err != nil {
	if rds.IsRedisNil(err) {
		fmt.Println("键不存在")
	} else {
		log.Fatal(err)
	}
}
```

## 性能考虑

- 根据工作负载使用适当的批处理大小
- 考虑对多个操作使用管道
- 监控大数据集的内存使用情况
- 对高吞吐量应用使用连接池

## 许可证

Apache 许可证 2.0 - 详见 [LICENSE](LICENSE) 文件。

## 贡献

欢迎贡献！请随时提交 Pull Request。

## 支持

如有问题和支持需求，请在 GitHub 上提交 issue。
