package cmd

import (
	"fmt"
	"golang.org/x/time/rate"
	"math"
	"math/rand"
	"strings"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const Inf = rate.Limit(math.MaxFloat64)

// readPreference int mapping
const (
	CommandTypeRead  int64 = 0
	CommandTypeWrite       = 1
)

type Datapoint struct {
	Error   bool
	Latency uint64
	Type    int64
}

type TestSpec struct {
	Rps                      uint64
	Clients                  uint64
	Duration                 time.Duration
	StartAtUnix              int64
	ReadPreference           int64
	NumReplicas              int64
	NumReplicasTimeoutMillis int64
	Addr                     string
	Password                 string
	Username                 string
	ReadProportion           float64
	InsertProportion         float64
	RecordCount              uint64
	DataSize                 uint64
}

func stringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func keyBuildLogic(keyPos int, dataPos int, datasize, keyspacelen uint64, cmdS []string, charset string) (newCmdS []string, key string) {
	newCmdS = make([]string, len(cmdS))
	copy(newCmdS, cmdS)
	if keyPos > -1 {
		keyV := fmt.Sprintf("%d", rand.Int63n(int64(keyspacelen)))
		key = strings.Replace(newCmdS[keyPos], "__key__", keyV, -1)
		newCmdS[keyPos] = key
	}
	if dataPos > -1 {
		newCmdS[dataPos] = stringWithCharset(int(datasize), charset)
	}
	return newCmdS, key
}
