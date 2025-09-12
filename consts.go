package rds

import "github.com/stephenfire/go-common"

const (
	ModelsBatchSize = 100
	BatchSize       = 500

	TypeKeyNone   = "none"
	TypeKeyStream = "stream"

	RdbStatusOK         = "OK"
	ErrBUSYGROUPKeyword = "BUSYGROUP"

	// DefaultStartScore zset default score range
	DefaultStartScore = "0"
	DefaultStopScore  = "+inf"

	StringPlaceHolder = "_"
	IdPlaceHolder     = 0
	NegIdPlaceHolder  = -1

	// HashExpireMinVersion min version of redis-server starting support HExpire
	HashExpireMinVersion = common.Version(7004000)
)
