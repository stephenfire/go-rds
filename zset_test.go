package rds

import (
	"testing"
)

func TestZArgs(t *testing.T) {

	tests := []struct {
		args   *ZArgs
		output string
	}{
		{
			args:   NewZArgs("zset_key").ByScore().Rev().Range(ZForScore().Val(0).Exclude(), ZForScore().Val(int64(1000000))),
			output: "zset_key 1000000 (0 BYSCORE REV",
		},
	}

	for _, test := range tests {
		s := test.args.String()
		if s != test.output {
			t.Errorf("want %s got %s", test.output, s)
		}
	}
}
