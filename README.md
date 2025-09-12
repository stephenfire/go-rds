# go-rds

A high-level Redis client library for Go that provides type-safe operations, batch processing, and enhanced Redis data structure support.

## Features

- **Type-safe operations**: Automatic encoding/decoding for various data types
- **Batch processing**: Efficient batch operations with configurable batch sizes
- **Redis data structures**: Comprehensive support for Strings, Hashes, Lists, Sets, and Sorted Sets
- **Distributed locking**: Built-in Redis lock implementation
- **Stream support**: Redis Stream operations for message queuing
- **Cache penetration protection**: Mechanisms to prevent cache penetration
- **Connection management**: Redis client with version detection and connection pooling

## Installation

```bash
go get github.com/stephenfire/go-rds
```

## Quick Start

### Basic Usage

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/stephenfire/go-rds"
)

func main() {
	ctx := context.Background()
	
	// Connect to Redis
	client, err := rds.ConnectRedis(ctx, "redis://localhost:6379")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Set a value
	err = client.Set(ctx, "mykey", "Hello, Redis!", 10*time.Minute)
	if err != nil {
		log.Fatal(err)
	}

	// Get a value
	value, exists, err := client.Get(ctx, "mykey", reflect.TypeOf(""))
	if err != nil {
		log.Fatal(err)
	}
	if exists {
		fmt.Println("Value:", value.(string))
	}
}
```

### String Operations

```go
// Create a string reader/writer
stringRW := rds.NewRedisString[string, string](
	client.C(),
	rds.RedisStringEncoder,
	rds.RedisStringDecoder,
	rds.RedisStringEncoder,
	rds.RedisStringDecoder,
)

// Set multiple values
err := stringRW.Sets(ctx, map[string]string{
	"key1": "value1",
	"key2": "value2",
	"key3": "value3",
})

// Get multiple values
notCached, cached, err := stringRW.Gets(ctx, "key1", "key2", "key3")
```

### Hash Operations

```go
// Create a hash reader/writer
hasher := rds.NewRedisHasher[string, int64](
	client.C(),
	rds.RedisStringEncoder, rds.RedisStringDecoder,
	rds.RedisInt64Encoder, rds.RedisInt64Decoder,
)

// Set hash values
err := hasher.HSetsMap(ctx, "user:scores", map[string]int64{
	"user1": 100,
	"user2": 200,
	"user3": 300,
})

// Get hash values
notCached, cached, err := hasher.HGets(ctx, "user:scores", "user1", "user2")
```

### Sorted Set Operations

```go
// Create a sorted set for ID scores
zset := rds.NewIdScoreZSet(client.C())

// Add scores to sorted set
scores := []*rds.IdScore{
	{Id: 1, Score: 100},
	{Id: 2, Score: 200},
	{Id: 3, Score: 300},
}

count, err := zset.ZAdd(ctx, "leaderboard", scores...)

// Query top 10 scores
args := &rds.ZArgs{}
args.WithKey("leaderboard").ByScore().Range(0, 1000).Paginate(0, 10).Rev()
topScores, err := zset.ZRangeWithScores(ctx, args)
```

### Distributed Locking

```go
// Create a Redis lock
locker := redislock.New(client.C())
redisLock := rds.NewRedisLock(client.C(), locker, "my-lock", "lock-value", 30*time.Second)

// Acquire lock
lockingValue, err := redisLock.Fetch(ctx)
if err != nil {
	log.Fatal("Failed to acquire lock")
}

// Do critical work
fmt.Println("Lock acquired, doing critical work...")

// Release lock
err = redisLock.Release()
if err != nil {
	log.Fatal("Failed to release lock")
}
```

### Stream Operations

```go
// Initialize stream group
err := client.InitStreamGroup(ctx, "mystream", "mygroup")

// Push messages to stream
messages := []map[string]interface{}{
	{"field1": "value1", "field2": 123},
	{"field1": "value2", "field2": 456},
}

err = client.QueuePushMaxLen(ctx, "mystream", 1000, messages...)

// Read messages from stream
msgs, err := client.QueueReadGroup(ctx, "mystream", "mygroup", "worker1", 5*time.Second, 10)
```

## Configuration

### Connection Options

```go
// Custom connection with specific timeouts
opts, err := redis.ParseURL("redis://localhost:6379")
if err != nil {
	log.Fatal(err)
}
opts.ReadTimeout = 2 * time.Second
opts.WriteTimeout = 5 * time.Second

client := redis.NewClient(opts)
redisClient := &rds.RedisClient{Client: client}
```

### Batch Size Configuration

```go
// Configure batch sizes for different operations
const (
	ModelsBatchSize = 100  // For hash operations
	BatchSize       = 500  // For sorted set operations
)
```

## Advanced Usage

### Custom Type Encoding/Decoding

```go
type User struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Custom encoder
func userEncoder(u User) (string, error) {
	return rds.RedisEncode(u)
}

// Custom decoder  
func userDecoder(s string) (User, error) {
	var u User
	err := rds.RedisDecode(reflect.TypeOf(u), s, &u)
	return u, err
}

// Use custom types
userHasher := rds.NewRedisHasher[int64, User](
	client.C(),
	rds.RedisInt64Encoder, rds.RedisInt64Decoder,
	userEncoder, userDecoder,
)
```

### Cache Penetration Protection

```go
// Using HGetsAndSetsIfNA to prevent cache penetration
flagField := int64(-1) // Special flag field
placeholderUser := User{ID: -1, Name: "", Email: ""}

users, err := userHasher.HGetsAndSetsIfNA(ctx, "users:cache", 
	func(ctx context.Context) (map[int64]User, error) {
		// Load all users from database
		return loadAllUsersFromDB(ctx)
	}, 
	flagField, 
	userID1, userID2, userID3,
)
```

## Error Handling

```go
// Check for Redis nil errors
value, exists, err := client.Get(ctx, "nonexistent", reflect.TypeOf(""))
if err != nil {
	if rds.IsRedisNil(err) {
		fmt.Println("Key does not exist")
	} else {
		log.Fatal(err)
	}
}
```

## Performance Considerations

- Use appropriate batch sizes for your workload
- Consider using pipeline for multiple operations
- Monitor memory usage for large datasets
- Use connection pooling for high-throughput applications

## License

Apache License 2.0 - See [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Support

For support and questions, please open an issue on GitHub.
