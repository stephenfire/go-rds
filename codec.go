package rds

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/stephenfire/go-rtl"
	"github.com/stephenfire/go-tools"
)

type (
	RedisDecoder[T any] func(r string) (T, error)

	// RedisEncoder returns "" means ignoring
	RedisEncoder[T any] func(t T) (string, error)

	// RedisZEncoder returns ok==false means ignoring the value
	RedisZEncoder[Z any] func(v Z) (z redis.Z, ok bool, err error)
	RedisZDecoder[Z any] func(z redis.Z) (v Z, err error)

	// RedisFloatEncoder encoder for ZSet score
	RedisFloatEncoder[S any] func(v S) (float64, error)
	// RedisFloatDecoder decoder for ZSet score
	RedisFloatDecoder[S any] func(f float64) (S, error)

	Encoder[T any, R any] func(t T) (R, error)
	Decoder[T any, R any] func(r R) (T, error)
)

func (e Encoder[T, R]) Encodes(ts ...T) ([]R, error) {
	if len(ts) == 0 {
		return nil, nil
	}
	rs := make([]R, len(ts))
	err := e.EncodesTo(rs, ts...)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func (e Encoder[T, R]) EncodesAsAny(ts ...T) ([]any, error) {
	if len(ts) == 0 {
		return nil, nil
	}
	rs := make([]any, len(ts))
	err := e.EncodesToAsAny(rs, ts...)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func (e Encoder[T, R]) EncodesTo(dest []R, ts ...T) error {
	if len(dest) == 0 {
		return nil
	}
	for i := 0; i < len(dest) && i < len(ts); i++ {
		v, err := e(ts[i])
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}

func (e Encoder[T, R]) EncodesToAsAny(dest []any, ts ...T) error {
	if len(dest) == 0 {
		return nil
	}
	for i := 0; i < len(dest) && i < len(ts); i++ {
		v, err := e(ts[i])
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}

func (d Decoder[T, R]) Decode(r R, rdsErr error) (t T, e error) {
	if rdsErr != nil {
		if IsRedisNil(rdsErr) {
			return t, nil
		}
		return t, rdsErr
	}
	return d(r)
}

func (d Decoder[T, R]) Decodes(rs []R, rdsErr error) ([]T, error) {
	if rdsErr != nil {
		if IsRedisNil(rdsErr) {
			return nil, nil
		}
		return nil, rdsErr
	}
	vs := make([]T, 0, len(rs))
	for _, r := range rs {
		v, err := d(r)
		if err != nil {
			return nil, err
		}
		vs = append(vs, v)
	}
	return vs, nil
}

func (e RedisEncoder[T]) Encodes(ts ...T) ([]string, error) {
	return Encoder[T, string](e).Encodes(ts...)
}

func (e RedisEncoder[T]) EncodesTo(dest []string, ts ...T) error {
	return Encoder[T, string](e).EncodesTo(dest, ts...)
}

func (d RedisDecoder[T]) Decode(str string, rdsErr error) (t T, e error) {
	return Decoder[T, string](d).Decode(str, rdsErr)
}

func (d RedisDecoder[T]) Decodes(strs []string, rdsErr error) ([]T, error) {
	return Decoder[T, string](d).Decodes(strs, rdsErr)
}

func TypeEncoder[T any](value T) (string, error) {
	val := reflect.ValueOf(value)
	if !val.IsValid() {
		return "", ErrInvalidValue
	}
	for val.Kind() == reflect.Pointer {
		val = val.Elem()
	}
	var str string
	switch {
	case val.Kind() == reflect.String:
		str = val.String()
	case val.Kind() == reflect.Slice && val.Type().Elem().Kind() == reflect.Uint8:
		str = string(val.Bytes())
	default:
		bs, err := rtl.Marshal(value)
		if err != nil {
			return "", fmt.Errorf("marshal value failed: %w", err)
		}
		str = string(bs)
	}
	return str, nil
}

func RedisEncode(value any) (string, error) {
	return TypeEncoder(value)
}

func TypeDecoder[T any](typ reflect.Type, strVal string) (t T, err error) {
	switch {
	case typ.Kind() == reflect.String:
		val := reflect.New(typ)
		eval := val.Elem()
		eval.SetString(strVal)
		return eval.Interface().(T), nil
	case tools.IsByteSlice(typ):
		val := reflect.New(typ)
		eval := val.Elem()
		if err = tools.SetByteSliceValue(eval, []byte(strVal)); err != nil {
			return t, err
		}
		return eval.Interface().(T), nil
	default:
		val := reflect.New(typ)
		if err := rtl.Unmarshal([]byte(strVal), val.Interface()); err != nil {
			return t, err
		}
		return val.Elem().Interface().(T), nil
	}
}

func RedisDecode(typ reflect.Type, strVal string) (any, error) {
	return TypeDecoder[any](typ, strVal)
}

func RedisValueDecode[T any](val any, decoder func(string) (T, error)) (exist bool, t T, err error) {
	if val == nil {
		return false, t, nil
	}
	if str, ok := val.(string); !ok {
		return false, t, errors.New("rds: expect string value")
	} else {
		t, err = decoder(str)
		return true, t, err
	}
}

func NewTypeDecoder[T any](typ reflect.Type) RedisDecoder[T] {
	return func(r string) (T, error) {
		return TypeDecoder[T](typ, r)
	}
}

func RedisStringDecoder(s string) (string, error) {
	return s, nil
}

func RedisStringEncoder(s string) (string, error) {
	return s, nil
}

func RedisUint64Decoder(s string) (uint64, error) {
	return strconv.ParseUint(s, 10, 64)
}

func RedisUint64Encoder(s uint64) (string, error) {
	return strconv.FormatUint(s, 10), nil
}

func RedisInt64Decoder(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func RedisInt64Encoder(s int64) (string, error) {
	return strconv.FormatInt(s, 10), nil
}

func RedisInt64SliceDecoder(s string) ([]int64, error) {
	return TypeDecoder[[]int64](tools.Int64SliceType, s)
}

func RedisInt64SliceEncoder(s []int64) (string, error) {
	return RedisEncode(s)
}

func RedisStringSliceDecoder(s string) ([]string, error) {
	return TypeDecoder[[]string](tools.StringSliceType, s)
}

func RedisStringSliceEncoder(s []string) (string, error) {
	return RedisEncode(s)
}

func Int64ToFloat64(i int64) (float64, error) {
	return float64(i), nil
}

func Float64ToInt64(f float64) (int64, error) {
	i, flowed := tools.F(f).Int()
	if flowed {
		if i < 0 {
			return i, errors.New("rds: underflowed")
		}
		return i, errors.New("rds: overflowed")
	}
	return i, nil
}
