package rds

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-common"
	"github.com/stephenfire/go-common/log"
	"github.com/stephenfire/go-tools"
)

type Z redis.Z

func NewZ(member string, score int64) redis.Z {
	return redis.Z{Score: float64(score), Member: member}
}

func NewTimeZ(id int64, ts time.Time) redis.Z {
	return redis.Z{Score: float64(ts.UnixMilli()), Member: tools.ID(id).PadZeros()}
}

func (z Z) Z() redis.Z {
	return redis.Z(z)
}

func (z Z) KeyStr() (string, error) {
	s, ok := z.Member.(string)
	if !ok {
		return "", ErrExpectingString
	}
	return s, nil
}

func (z Z) KeyID() (tools.ID, error) {
	s, err := z.KeyStr()
	if err != nil {
		return 0, err
	}
	return tools.S(s).ID()
}

func (z Z) ScoreInt() int64 {
	return tools.F(z.Score).MustInt()
}

func (z Z) IsEmpty() bool {
	s := z.Member.(string)
	return s == "0"
}

func (z Z) IdAndValue() (int64, int64, error) {
	id, err := z.KeyID()
	if err != nil {
		return 0, 0, err
	}
	return id.Int(), z.ScoreInt(), nil
}

var EmptyZ = redis.Z{Score: -1, Member: "0"}

type Zs[T any] []redis.Z

func (zs Zs[T]) Decode(decoder RedisZDecoder[T]) ([]T, error) {
	if len(zs) == 0 {
		return nil, nil
	}
	ret := make([]T, 0, len(zs))
	for _, z := range zs {
		t, err := decoder(z)
		if err != nil {
			return nil, err
		}
		ret = append(ret, t)
	}
	return ret, nil
}

func (zs Zs[T]) Encode(encoder RedisZEncoder[T], ts ...T) (Zs[T], error) {
	if len(zs) == 0 {
		return nil, nil
	}
	ret := make(Zs[T], 0, len(ts))
	for _, t := range ts {
		z, ok, err := encoder(t)
		if err != nil {
			return nil, err
		}
		if ok {
			ret = append(ret, z)
		}
	}
	return ret, nil
}

func (zs Zs[T]) Slice() []redis.Z {
	return ([]redis.Z)(zs)
}

type RedisLock struct {
	redis  *redis.Client
	locker *redislock.Client
	key    string
	value  string
	ttl    time.Duration
	rlock  *redislock.Lock
	lock   sync.Mutex
}

func NewRedisLock(client *redis.Client, locker *redislock.Client, key, value string, ttl time.Duration) *RedisLock {
	return &RedisLock{
		redis:  client,
		locker: locker,
		key:    key,
		value:  value,
		ttl:    ttl,
		rlock:  nil,
	}
}

func (l *RedisLock) String() string {
	if l == nil {
		return "RedisLock<nil>"
	}
	return fmt.Sprintf("RedisLock{%s ttl:%s}", l.key, l.ttl)
}

func (l *RedisLock) _fetch(cctx context.Context) (lockingValue string, err error) {
	ctx, cancel := context.WithTimeout(cctx, RedisTimeout)
	defer cancel()
	var lock *redislock.Lock
	lock, err = l.locker.Obtain(ctx, l.key, l.ttl, &redislock.Options{Token: l.value})
	if err != nil {
		if errors.Is(err, redislock.ErrNotObtained) {
			lockingValue = l.redis.Get(ctx, l.key).Val()
		}
		log.Debugf("RedisLock: %s optain lock failed: %s, %v", l, lockingValue, err)
	} else {
		l.rlock = lock
		lockingValue = lock.Token()
		log.Debugf("RedisLock: %s obtained lock for key: %s", l, lockingValue)
	}
	return
}

func (l *RedisLock) Fetch(cctx context.Context) (lockingValue string, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.rlock != nil {
		return l.rlock.Token(), errors.New("lock already fetched")
	}
	defer func() {
		if err == nil {
			log.Debugf("%s lock success", l)
		}
	}()
	return l._fetch(cctx)
}

func (l *RedisLock) Release() (err error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.rlock == nil {
		return nil
	}
	defer func() {
		if err != nil {
			log.Warnf("%s release failed: %v", l, err)
		} else {
			log.Debugf("%s released", l)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), RedisTimeout)
	defer cancel()
	err = l.rlock.Release(ctx)
	l.rlock = nil
	return err
}

func (l *RedisLock) _refresh(cctx context.Context) error {
	ctx, cancel := context.WithTimeout(cctx, RedisTimeout)
	defer cancel()
	return l.rlock.Refresh(ctx, l.ttl, nil)
}

func (l *RedisLock) Refresh(cctx context.Context) (err error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	defer func() {
		if err != nil {
			log.Warnf("%s refresh failed: %v", l, err)
		} else {
			log.Debugf("%s refreshed", l)
		}
	}()
	if l.rlock == nil {
		return errors.New("lock not fetched")
	}
	return l._refresh(cctx)
}

func (l *RedisLock) FetchOrRefresh(cctx context.Context) (lockingValue string, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.rlock == nil {
		return l._fetch(cctx)
	} else {
		err = l._refresh(cctx)
		if err != nil {
			return
		}
		return l.value, nil
	}
}

type RedisMsg map[string]interface{}

func (m RedisMsg) IsEmpty() bool {
	return len(m) == 0
}

func (m RedisMsg) IsStopSignal() bool {
	v, ok := m["stop_signal"]
	if !ok {
		return false
	}
	val, ok := v.(string)
	if !ok || val != "stop" {
		return false
	}
	return true
}

func (m RedisMsg) StopSignal() RedisMsg {
	return RedisMsg{"stop_signal": "stop"}
}

func (m RedisMsg) Int64(key string, nilIsOk ...bool) (i int64, err error) {
	nilok := len(nilIsOk) > 0 && nilIsOk[0]
	if len(m) == 0 {
		if nilok {
			return 0, nil
		}
		return 0, fmt.Errorf("id of %s: %w", key, common.ErrNotFound)
	}
	v, ok := m[key]
	if !ok {
		if nilok {
			return 0, nil
		}
		return 0, fmt.Errorf("id of %s: %w", key, common.ErrNotFound)
	}
	if v == nil {
		if nilok {
			return 0, nil
		}
		return 0, fmt.Errorf("id of %s: %w", key, common.ErrNotFound)
	}
	s, ok := v.(string)
	if !ok || s == "" {
		if nilok {
			return 0, nil
		}
		return 0, fmt.Errorf("id of %s: %w", key, common.ErrNotFound)
	}
	return tools.S(s).Int64()
}

func (m RedisMsg) ID(key string, nilIsOk ...bool) (tools.ID, error) {
	i, err := m.Int64(key, nilIsOk...)
	if err != nil {
		return 0, err
	}
	return tools.ID(i), nil
}

func (m RedisMsg) String(key string, nilIsOk ...bool) (string, error) {
	nilok := len(nilIsOk) > 0 && nilIsOk[0]
	if len(m) == 0 {
		if nilok {
			return "", nil
		}
		return "", fmt.Errorf("string of %s: %w", key, common.ErrNotFound)
	}
	v, ok := m[key]
	if !ok {
		if nilok {
			return "", nil
		}
		return "", fmt.Errorf("string of %s: %w", key, common.ErrNotFound)
	}
	if v == nil {
		if nilok {
			return "", nil
		}
		return "", fmt.Errorf("string of %s: %w", key, common.ErrNotFound)
	}
	s, ok := v.(string)
	if !ok {
		if nilok {
			return "", nil
		}
		return "", fmt.Errorf("string of %s: %w", key, common.ErrNotFound)
	}
	return s, nil
}

func (m RedisMsg) Map() map[string]interface{} {
	return m
}
