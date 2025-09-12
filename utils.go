package rds

import (
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"
)

func IsRedisNil(err error) bool {
	return errors.Is(err, redis.Nil)
}

func IsRedisNoGroup(err error) bool {
	errstr := err.Error()
	return strings.Contains(errstr, "NOGROUP") && strings.Contains(errstr, "XREADGROUP")
}
