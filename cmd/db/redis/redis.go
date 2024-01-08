package redis

import (
	"context"
	"fmt"
	"github.com/filipecosta90/global-nosql-benchmark/cmd"
	goredis "github.com/go-redis/redis/v9"
	"github.com/magiconair/properties"
)

const HSET string = "HSET"
const HMGET string = "HMGET"
const WAIT string = "WAIT"

type redisClient interface {
	Do(ctx context.Context, args ...interface{}) *goredis.Cmd
	Pipeline() goredis.Pipeliner
	FlushDB(ctx context.Context) *goredis.StatusCmd
	Close() error
}

type redis struct {
	client redisClient
}

func (r *redis) Read(ctx context.Context, table string, key string, readPreference int64, fields []string) (data map[string][]byte, err error) {
	data = make(map[string][]byte, len(fields))
	err = nil
	args := make([]interface{}, 0, len(fields)+2)
	args = append(args, HMGET, getKeyName(table, key))
	for _, fieldName := range fields {
		args = append(args, fieldName)
	}
	sliceReply, errI := r.client.Do(ctx, args...).StringSlice()
	if errI != nil {
		return
	}
	for pos, slicePos := range sliceReply {
		data[fields[pos]] = []byte(slicePos)
	}

	return

}

func getKeyName(table string, key string) string {
	return table + "/" + key
}

func (r *redis) Insert(ctx context.Context, table string, key string, numReplicas int64, numReplicasTimeout int64, values map[string][]byte) (err error) {

	args := make([]interface{}, 0, 2*len(values)+2)
	argsWait := make([]interface{}, 0, 3)
	args = append(args, HSET, getKeyName(table, key))
	argsWait = append(argsWait, WAIT, fmt.Sprintf("%d", numReplicas), fmt.Sprintf("%d", numReplicasTimeout))
	for fieldName, bytes := range values {
		args = append(args, fieldName, string(bytes))
	}
	if numReplicas < 1 {
		err = r.client.Do(ctx, args...).Err()
	} else {
		pipe := r.client.Pipeline()
		pipe.Do(ctx, args...)
		pipe.Do(ctx, argsWait...)
		_, err = pipe.Exec(ctx)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	return
}

type redisCreator struct{}

func (r redisCreator) Create(p *properties.Properties) (cmd.DB, error) {
	rds := &redis{}

	redisOptions := goredis.Options{
		Addr:     "localhost:6379",
		Password: "",
	}

	singleEndpointClient := goredis.NewClient(&redisOptions)
	err := singleEndpointClient.Ping(context.Background()).Err()
	if err != nil {
		return nil, err
	}
	rds.client = singleEndpointClient
	return rds, nil
}

func init() {
	cmd.RegisterDBCreator("redis", redisCreator{})
}
